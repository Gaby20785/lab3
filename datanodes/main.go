package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"sync"
	"time"

	"google.golang.org/grpc"

	pb "lab3/datanodes/proto"
)

type datanodeServer struct {
	pb.UnimplementedAeroDistServer
	mu                sync.RWMutex
	datanodeID        string
	puerto            string
	brokerConn        pb.AeroDistClient
	vuelosDisponibles []string
	estadoVuelos      map[string]*EstadoVuelo
	reservas          map[string]*Reserva
	vectorClock       map[string]int32
	knownNodes        []string
	gossipTicker      *time.Ticker
}

type EstadoVuelo struct {
	UltimoEstado       string
	UltimaPuerta       string
	RelojVectorial     map[string]int32
	UltimaActualizacion time.Time
	Asientos          map[string]bool
	Tarjetas          map[string]*TarjetaEmbarque
}

type Reserva struct {
	VueloID   string
	Asiento   string
	ClienteID string
	Timestamp time.Time
}

type TarjetaEmbarque struct {
	VueloID   string
	Asiento   string
	ClienteID string
	Timestamp time.Time
}

func fusionarRelojes(relojA, relojB map[string]int32) map[string]int32 {
	fusionado := make(map[string]int32)
	
	for nodo, valor := range relojA {
		fusionado[nodo] = valor
	}
	
	for nodo, valorB := range relojB {
		if valorA, existe := fusionado[nodo]; !existe || valorB > valorA {
			fusionado[nodo] = valorB
		}
	}
	
	return fusionado
}

func compararRelojes(relojA, relojB map[string]int32) string {
	aEsMayorEnAlguno := false
	bEsMayorEnAlguno := false
	
	todosNodos := make(map[string]bool)
	for nodo := range relojA { 
		todosNodos[nodo] = true 
	}
	for nodo := range relojB { 
		todosNodos[nodo] = true 
	}
	
	for nodo := range todosNodos {
		valorA := relojA[nodo]
		valorB := relojB[nodo]
		
		if valorA > valorB {
			aEsMayorEnAlguno = true
		} else if valorB > valorA {
			bEsMayorEnAlguno = true
		}
	}
	
	if aEsMayorEnAlguno && !bEsMayorEnAlguno {
		return "posterior" // A > B
	} else if !aEsMayorEnAlguno && bEsMayorEnAlguno {
		return "anterior"  // A < B
	} else if aEsMayorEnAlguno && bEsMayorEnAlguno {
		return "concurrente" // A || B
	}
	return "igual" // A = B
}

func (s *datanodeServer) prioridadEstado(estado string) int {
	prioridades := map[string]int{
		"Cancelado":    100,
		"Desviado":     90,
		"Emergencia":   80,
		"Aterrizando":  70,
		"Despegando":   60,
		"Embarcando":   50,
		"Retrasado":    40,
		"En vuelo":     30,
		"A tiempo":     20,
		"Programado":   10,
		"":             0,
	}
	
	if prio, existe := prioridades[estado]; existe {
		return prio
	}
	return 0
}

func (s *datanodeServer) resolverConflicto(
	vueloID string, 
	estadoLocal *EstadoVuelo, 
	update *pb.ActualizacionVueloRequest,
) {
	log.Printf("%s: resolviendo conflicto para vuelo %s", s.datanodeID, vueloID)
	
	if update.UpdateType == "estado" {
		if estadoLocal.UltimoEstado == "" {
			estadoLocal.UltimoEstado = update.UpdateValue
			log.Printf("%s: Aceptado estado '%s' (primera actualización)", s.datanodeID, update.UpdateValue)
		} else {
			if s.prioridadEstado(update.UpdateValue) > s.prioridadEstado(estadoLocal.UltimoEstado) {
				estadoLocal.UltimoEstado = update.UpdateValue
				log.Printf("%s: Aceptado estado '%s' por mayor prioridad", s.datanodeID, update.UpdateValue)
			} else if s.prioridadEstado(update.UpdateValue) == s.prioridadEstado(estadoLocal.UltimoEstado) {
				if update.SimTimeSec > estadoLocal.UltimaActualizacion.Unix() {
					estadoLocal.UltimoEstado = update.UpdateValue
					log.Printf("%s: Aceptado estado '%s' por timestamp más reciente", s.datanodeID, update.UpdateValue)
				} else {
					log.Printf("%s: Mantenido estado local '%s'", s.datanodeID, estadoLocal.UltimoEstado)
				}
			} else {
				log.Printf("%s: Mantenido estado local '%s' por mayor prioridad", s.datanodeID, estadoLocal.UltimoEstado)
			}
		}
	} else if update.UpdateType == "puerta" {
		if update.SimTimeSec > estadoLocal.UltimaActualizacion.Unix() {
			estadoLocal.UltimaPuerta = update.UpdateValue
			log.Printf("%s: Aceptada puerta '%s' por timestamp más reciente", s.datanodeID, update.UpdateValue)
		} else {
			log.Printf("%s: Mantenida puerta local '%s'", s.datanodeID, estadoLocal.UltimaPuerta)
		}
	}
	
	estadoLocal.RelojVectorial = fusionarRelojes(
		estadoLocal.RelojVectorial, 
		update.RelojVector.Clocks,
	)
	estadoLocal.UltimaActualizacion = time.Unix(update.SimTimeSec, 0)
}

func (s *datanodeServer) generarAsientos() map[string]bool {
	asientos := make(map[string]bool)
	for fila := 1; fila <= 30; fila++ {
		for letra := 'A'; letra <= 'F'; letra++ {
			asiento := fmt.Sprintf("%d%c", fila, letra)
			asientos[asiento] = false
		}
	}
	return asientos
}

func (s *datanodeServer) inicializarEstado() {
	s.estadoVuelos = make(map[string]*EstadoVuelo)
	s.reservas = make(map[string]*Reserva)
	
	for _, vuelo := range s.vuelosDisponibles {
		s.estadoVuelos[vuelo] = &EstadoVuelo{
			Asientos: s.generarAsientos(),
			Tarjetas: make(map[string]*TarjetaEmbarque),
			RelojVectorial: make(map[string]int32),
		}
	}
	log.Printf("Datanode %s: Estado inicializado para %d vuelos", s.datanodeID, len(s.vuelosDisponibles))
}

func (s *datanodeServer) ObtenerAsientosDisponibles(ctx context.Context, req *pb.AsientosRequest) (*pb.AsientosResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	log.Printf("Datanode %s: Obtener asientos para vuelo %s", s.datanodeID, req.VueloId)

	estado, exists := s.estadoVuelos[req.VueloId]
	if !exists {
		return &pb.AsientosResponse{
			Exito:   false,
			Mensaje: fmt.Sprintf("Vuelo %s no encontrado", req.VueloId),
		}, nil
	}

	asientosCopia := make(map[string]bool)
	for asiento, ocupado := range estado.Asientos {
		asientosCopia[asiento] = ocupado
	}

	return &pb.AsientosResponse{
		Exito:          true,
		Mensaje:       fmt.Sprintf("Asientos del vuelo %s", req.VueloId),
		EstadoAsientos: asientosCopia,
	}, nil
}

func (s *datanodeServer) ReservarAsiento(ctx context.Context, req *pb.ReservaRequest) (*pb.ReservaResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	log.Printf("Datanode %s: Reservar asiento %s en vuelo %s para cliente %s", 
		s.datanodeID, req.Asiento, req.VueloId, req.ClienteId)

	s.vectorClock[s.datanodeID]++

	if reserva, exists := s.reservas[req.RequestId]; exists {
		log.Printf("Request ID duplicado %s, retornando respuesta anterior", req.RequestId)
		tarjetaID := fmt.Sprintf("TE-%s-%s-%s", reserva.ClienteID, reserva.VueloID, reserva.Asiento)
		return &pb.ReservaResponse{
			Exito:             true,
			Mensaje:           "Reserva ya procesada (idempotente)",
			TarjetaEmbarqueId: tarjetaID,
		}, nil
	}

	estado, exists := s.estadoVuelos[req.VueloId]
	if !exists {
		return &pb.ReservaResponse{
			Exito:   false,
			Mensaje: fmt.Sprintf("Vuelo %s no encontrado", req.VueloId),
		}, nil
	}

	if estado.Asientos[req.Asiento] {
		return &pb.ReservaResponse{
			Exito:   false,
			Mensaje: fmt.Sprintf("Asiento %s ya está ocupado", req.Asiento),
		}, nil
	}

	estado.Asientos[req.Asiento] = true
	tarjetaID := fmt.Sprintf("TE-%s-%s-%s", req.ClienteId, req.VueloId, req.Asiento)
	
	estado.Tarjetas[tarjetaID] = &TarjetaEmbarque{
		VueloID:   req.VueloId,
		Asiento:   req.Asiento,
		ClienteID: req.ClienteId,
		Timestamp: time.Now(),
	}

	if estado.RelojVectorial == nil {
		estado.RelojVectorial = make(map[string]int32)
	}
	estado.RelojVectorial[s.datanodeID] = s.vectorClock[s.datanodeID]
	estado.UltimaActualizacion = time.Now()

	s.reservas[req.RequestId] = &Reserva{
		VueloID:   req.VueloId,
		Asiento:   req.Asiento,
		ClienteID: req.ClienteId,
		Timestamp: time.Now(),
	}

	log.Printf("Datanode %s: Asiento %s reservado exitosamente (reloj: %v)", 
		s.datanodeID, req.Asiento, s.vectorClock)

	return &pb.ReservaResponse{
		Exito:             true,
		Mensaje:           "Asiento reservado exitosamente",
		TarjetaEmbarqueId: tarjetaID,
	}, nil
}

func (s *datanodeServer) ObtenerInformacionVuelo(ctx context.Context, req *pb.InfoVueloRequest) (*pb.InfoVueloResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	log.Printf("Datanode %s: Obtener información para tarjeta %s", s.datanodeID, req.TarjetaEmbarqueId)

	for vueloID, estado := range s.estadoVuelos {
		if tarjeta, exists := estado.Tarjetas[req.TarjetaEmbarqueId]; exists {
			return &pb.InfoVueloResponse{
				Exito:     true,
				Mensaje:   "Información obtenida",
				VueloId:   vueloID,
				Asiento:   tarjeta.Asiento,
				ClienteId: tarjeta.ClienteID,
				Estado:    "Confirmado",
			}, nil
		}
	}

	return &pb.InfoVueloResponse{
		Exito:   false,
		Mensaje: "Tarjeta de embarque no encontrada",
	}, nil
}

func (s *datanodeServer) RegistrarEntidad(ctx context.Context, req *pb.RegistroRequest) (*pb.RegistroResponse, error) {
	return &pb.RegistroResponse{Exito: true, Mensaje: "OK"}, nil
}

func (s *datanodeServer) SolicitarInicio(ctx context.Context, req *pb.InicioRequest) (*pb.InicioResponse, error) {
	return &pb.InicioResponse{PuedeIniciar: true, Mensaje: "Datanode listo"}, nil
}

func (s *datanodeServer) EnviarActualizacionVuelo(ctx context.Context, req *pb.ActualizacionVueloRequest) (*pb.ActualizacionVueloResponse, error) {
	log.Printf("%s: Recibiendo update [%s] %s=%s (reloj: %v)", 
		s.datanodeID, req.VueloId, req.UpdateType, req.UpdateValue, req.RelojVector.Clocks)

	s.mu.Lock()
	defer s.mu.Unlock()

	relojMensaje := make(map[string]int32)
	for k, v := range req.RelojVector.Clocks {
		relojMensaje[k] = v
	}
	
	nuevoRelojGlobal := fusionarRelojes(s.vectorClock, relojMensaje)
	s.vectorClock = nuevoRelojGlobal

	if _, exists := s.estadoVuelos[req.VueloId]; !exists {
		s.estadoVuelos[req.VueloId] = &EstadoVuelo{
			Asientos:         s.generarAsientos(),
			Tarjetas:         make(map[string]*TarjetaEmbarque),
			RelojVectorial:   make(map[string]int32),
			UltimaActualizacion: time.Unix(req.SimTimeSec, 0),
		}
		s.vuelosDisponibles = append(s.vuelosDisponibles, req.VueloId)
		log.Printf("%s: Nuevo vuelo creado: %s", s.datanodeID, req.VueloId)
	}

	estado := s.estadoVuelos[req.VueloId]

	relacion := compararRelojes(estado.RelojVectorial, relojMensaje)
	
	switch relacion {
	case "anterior":
		if req.UpdateType == "estado" {
			estado.UltimoEstado = req.UpdateValue
		} else if req.UpdateType == "puerta" {
			estado.UltimaPuerta = req.UpdateValue
		}
		estado.RelojVectorial = fusionarRelojes(estado.RelojVectorial, relojMensaje)
		estado.UltimaActualizacion = time.Unix(req.SimTimeSec, 0)
		log.Printf("%s: Update causal aplicado [%s] %s=%s", s.datanodeID, req.VueloId, req.UpdateType, req.UpdateValue)
		
	case "concurrente":
		s.resolverConflicto(req.VueloId, estado, req)
		
	case "posterior", "igual":
		log.Printf("%s: Update ignorado [%s] (relación: %s)", s.datanodeID, req.VueloId, relacion)
	}

	respReloj := &pb.VectorClock{Clocks: s.vectorClock}
	return &pb.ActualizacionVueloResponse{
		Exito:       true,
		Mensaje:     "Update procesado correctamente",
		NuevoReloj: respReloj,
	}, nil
}

func (s *datanodeServer) sincronizarAsientos(estadoLocal *EstadoVuelo, estadoRemoto *pb.EstadoVuelo) bool {
	actualizado := false
    
    for asientoID, ocupadoRemoto := range estadoRemoto.AsientosOcupados {
        ocupadoLocal := estadoLocal.Asientos[asientoID]
        
        if ocupadoRemoto && !ocupadoLocal {
            estadoLocal.Asientos[asientoID] = true
            actualizado = true
            log.Printf("%s: Asiento %s marcado como ocupado desde gossip", s.datanodeID, asientoID)
        }
    }
    
    return actualizado
}

func (s *datanodeServer) sincronizarTarjetas(estadoLocal *EstadoVuelo, estadoRemoto *pb.EstadoVuelo) bool {
	actualizado := false
    
    for tarjetaID, tarjetaRemota := range estadoRemoto.Tarjetas {
        if _, existe := estadoLocal.Tarjetas[tarjetaID]; !existe {
            estadoLocal.Tarjetas[tarjetaID] = &TarjetaEmbarque{
                VueloID:   tarjetaRemota.VueloId,
                Asiento:   tarjetaRemota.Asiento,
                ClienteID: tarjetaRemota.ClienteId,
                Timestamp: time.Unix(tarjetaRemota.Timestamp, 0),
            }
            actualizado = true
            log.Printf("%s: Tarjeta %s agregada desde gossip", s.datanodeID, tarjetaID)
        }
    }
    
    return actualizado
}

func (s *datanodeServer) prepararGossip() *pb.EstadoDatanode {
	estados := make(map[string]*pb.EstadoVuelo)
    
    for vueloID, estado := range s.estadoVuelos {
        asientosOcupados := make(map[string]bool)
        for asientoID, ocupado := range estado.Asientos {
            asientosOcupados[asientoID] = ocupado
        }
        
        tarjetas := make(map[string]*pb.TarjetaInfo)
        for tarjetaID, tarjeta := range estado.Tarjetas {
            tarjetas[tarjetaID] = &pb.TarjetaInfo{
                VueloId:   tarjeta.VueloID,
                Asiento:   tarjeta.Asiento,
                ClienteId: tarjeta.ClienteID,
                Timestamp: tarjeta.Timestamp.Unix(),
            }
        }
        
        estados[vueloID] = &pb.EstadoVuelo{
            UltimoEstado:   estado.UltimoEstado,
            UltimaPuerta:   estado.UltimaPuerta,
            RelojEstado:    &pb.VectorClock{Clocks: estado.RelojVectorial},
            UltimaActualizacion: estado.UltimaActualizacion.Unix(),
            AsientosOcupados: asientosOcupados,
            Tarjetas:        tarjetas,
        }
    }

    return &pb.EstadoDatanode{
        EstadosVuelos: estados,
        RelojVector:   &pb.VectorClock{Clocks: s.vectorClock},
        DatanodeId:    s.datanodeID,
    }
}

func (s *datanodeServer) relojesIguales(relojA, relojB map[string]int32) bool {
	if len(relojA) != len(relojB) {
		return false
	}
	
	for k, vA := range relojA {
		if vB, existe := relojB[k]; !existe || vA != vB {
			return false
		}
	}
	return true
}

func (s *datanodeServer) procesarGossip(estadoRemitente *pb.EstadoDatanode) bool {
    s.mu.Lock()
    defer s.mu.Unlock()

    actualizado := false

    nuevoRelojGlobal := fusionarRelojes(s.vectorClock, estadoRemitente.RelojVector.Clocks)
    if !s.relojesIguales(s.vectorClock, nuevoRelojGlobal) {
        s.vectorClock = nuevoRelojGlobal
        actualizado = true
        log.Printf("%s: Reloj global actualizado desde gossip: %v", s.datanodeID, s.vectorClock)
    }

    for vueloID, estadoRemoto := range estadoRemitente.EstadosVuelos {
        if _, exists := s.estadoVuelos[vueloID]; !exists {
            s.estadoVuelos[vueloID] = &EstadoVuelo{
                Asientos:         s.generarAsientos(),
                Tarjetas:         make(map[string]*TarjetaEmbarque),
                RelojVectorial:   make(map[string]int32),
                UltimaActualizacion: time.Unix(estadoRemoto.UltimaActualizacion, 0),
            }
            s.vuelosDisponibles = append(s.vuelosDisponibles, vueloID)
            actualizado = true
            log.Printf("%s: Nuevo vuelo desde gossip: %s", s.datanodeID, vueloID)
        }

        estadoLocal := s.estadoVuelos[vueloID]
        
        asientosActualizados := s.sincronizarAsientos(estadoLocal, estadoRemoto)
        if asientosActualizados {
            actualizado = true
        }
        
        tarjetasActualizadas := s.sincronizarTarjetas(estadoLocal, estadoRemoto)
        if tarjetasActualizadas {
            actualizado = true
        }

        relojRemoto := estadoRemoto.RelojEstado.Clocks
        relacion := compararRelojes(estadoLocal.RelojVectorial, relojRemoto)

        if relacion == "anterior" || relacion == "concurrente" {
            timestampRemoto := time.Unix(estadoRemoto.UltimaActualizacion, 0)
            
            if relacion == "concurrente" {
                updateSimulado := &pb.ActualizacionVueloRequest{
                    VueloId:     vueloID,
                    UpdateType:  "estado",
                    UpdateValue: estadoRemoto.UltimoEstado,
                    SimTimeSec:  estadoRemoto.UltimaActualizacion,
                    RelojVector: &pb.VectorClock{Clocks: relojRemoto},
                }
                s.resolverConflicto(vueloID, estadoLocal, updateSimulado)
            } else {
                estadoLocal.UltimoEstado = estadoRemoto.UltimoEstado
                estadoLocal.UltimaPuerta = estadoRemoto.UltimaPuerta
                estadoLocal.RelojVectorial = fusionarRelojes(estadoLocal.RelojVectorial, relojRemoto)
                estadoLocal.UltimaActualizacion = timestampRemoto
            }
            
            actualizado = true
            log.Printf("%s: Estado actualizado desde gossip [%s]", s.datanodeID, vueloID)
        }
    }

    return actualizado
}

func (s *datanodeServer) SincronizarEstado(ctx context.Context, req *pb.SincronizacionRequest) (*pb.SincronizacionResponse, error) {
	log.Printf("%s: Recibiendo gossip de %s", s.datanodeID, req.DatanodeOrigen)

	actualizado := s.procesarGossip(req.EstadoRemitente)
	
	if actualizado {
		log.Printf("%s: Estado actualizado desde gossip", s.datanodeID)
	}

	respEstado := s.prepararGossip()

	return &pb.SincronizacionResponse{
		Exito:            true,
		Mensaje:          "Gossip procesado",
		EstadoActualizado: respEstado,
	}, nil
}

func (s *datanodeServer) iniciarGossip() {
	s.gossipTicker = time.NewTicker(5 * time.Second)
	go func() {
		for range s.gossipTicker.C {
			s.ejecutarCicloGossip()
		}
	}()
}

func (s *datanodeServer) ejecutarCicloGossip() {
	if len(s.knownNodes) == 0 {
		return
	}

	targetNode := s.knownNodes[rand.Intn(len(s.knownNodes))]
	
	s.mu.RLock()
	estadoActual := s.prepararGossip()
	s.mu.RUnlock()

	log.Printf("%s: Iniciando gossip con %s", s.datanodeID, targetNode)

	conn, err := grpc.Dial(targetNode, grpc.WithInsecure(), grpc.WithTimeout(3*time.Second))
	if err != nil {
		log.Printf("%s: Error conectando para gossip: %v", s.datanodeID, err)
		return
	}
	defer conn.Close()

	client := pb.NewAeroDistClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.SincronizarEstado(ctx, &pb.SincronizacionRequest{
		EstadoRemitente: estadoActual,
		DatanodeOrigen:  s.datanodeID,
	})

	if err != nil {
		log.Printf("%s: Error en gossip con %s: %v", s.datanodeID, targetNode, err)
		return
	}

	if resp.EstadoActualizado != nil {
		s.procesarGossip(resp.EstadoActualizado)
		log.Printf("%s: Gossip exitoso con %s - estado actualizado", s.datanodeID, targetNode)
	}
}

func (s *datanodeServer) ObtenerEstadoVuelo(ctx context.Context, req *pb.EstadoVueloRequest) (*pb.EstadoVueloResponse, error) {
	log.Printf("%s: Cliente %s consulta vuelo %s (versión: %v)", 
		s.datanodeID, req.ClienteId, req.VueloId, req.VersionCliente.GetClocks())

	if req.VersionCliente != nil && len(req.VersionCliente.Clocks) > 0 {
		startTime := time.Now()
		maxWait := 8 * time.Second
		
		for !s.esVersionNueva(req.VersionCliente.Clocks) {
			if time.Since(startTime) > maxWait {
				log.Printf("%s: Timeout esperando versión para MR", s.datanodeID)
				break
			}
			time.Sleep(500 * time.Millisecond)
		}
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	estado, exists := s.estadoVuelos[req.VueloId]
	if !exists {
		return &pb.EstadoVueloResponse{
			Exito:   false,
			Mensaje: fmt.Sprintf("Vuelo %s no encontrado", req.VueloId),
		}, nil
	}

	s.vectorClock[s.datanodeID]++

	estadoVuelo := "Desconocido"
	puertaVuelo := "N/A"

	if estado.UltimoEstado != "" {
		estadoVuelo = estado.UltimoEstado
	}
	if estado.UltimaPuerta != "" {
		puertaVuelo = estado.UltimaPuerta
	}

	if req.VersionCliente != nil {
		if s.verificarMonotonicidad(req.VersionCliente.Clocks, s.vectorClock) {
			log.Printf("%s: MONOTONIC READS GARANTIZADO para %s", s.datanodeID, req.ClienteId)
		} else {
			log.Printf("%s: POSIBLE VIOLACIÓN MR - continuando igual", s.datanodeID)
		}
	}

	return &pb.EstadoVueloResponse{
		Exito:         true,
		Mensaje:      "Estado obtenido correctamente",
		VueloId:      req.VueloId,
		Estado:       estadoVuelo,
		Puerta:       puertaVuelo,
		VersionActual: &pb.VectorClock{Clocks: s.vectorClock},
	}, nil
}

func (s *datanodeServer) esVersionNueva(relojCliente map[string]int32) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	for nodoCliente, valorCliente := range relojCliente {
		if valorNuestro, existe := s.vectorClock[nodoCliente]; existe {
			if valorNuestro < valorCliente {
				return false
			}
		} else {
			return false
		}
	}
	
	return true
}

func (s *datanodeServer) verificarMonotonicidad(relojCliente, relojDatanode map[string]int32) bool {
	for nodo, valorCliente := range relojCliente {
		if valorDatanode, existe := relojDatanode[nodo]; existe {
			if valorDatanode < valorCliente {
				return false
			}
		}
	}
	return true
}

func cargarFlightUpdates() []string {
	ruta := "flight_updates.csv"

	file, err := os.Open(ruta)
	if err != nil {
		log.Printf("No se pudo abrir flight_updates.csv")
		return []string{}
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		log.Printf("Error leyendo CSV: %v", err)
		return []string{}
	}

	vuelosMap := make(map[string]bool)
	
	for i, record := range records {
		if i == 0 {
			continue
		}
		
		if len(record) >= 2 {
			flightID := record[1]
			if flightID != "" {
				vuelosMap[flightID] = true
			}
		}
	}

	vuelos := make([]string, 0, len(vuelosMap))
	for id := range vuelosMap {
		vuelos = append(vuelos, id)
	}

	log.Printf("%d vuelos únicos cargados", len(vuelos))
	return vuelos
}

func conectarConBroker(address string) pb.AeroDistClient {
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("No se pudo conectar con broker: %v", err)
	}
	return pb.NewAeroDistClient(conn)
}

func (s *datanodeServer) inicializarRelojVectorial() {
	s.vectorClock = make(map[string]int32)
	s.vectorClock[s.datanodeID] = 0
	s.vectorClock["broker"] = 0
}

func (s *datanodeServer) esperarInicio() {
	ctx := context.Background()
	
	log.Printf("Datanode %s esperando autorización para iniciar...", s.datanodeID)
	for {
		resp, err := s.brokerConn.SolicitarInicio(ctx, &pb.InicioRequest{
			TipoEntidad: "datanode",
			IdEntidad:   s.datanodeID,
		})
		if err != nil {
			log.Printf("Datanode %s: Error solicitando inicio: %v", s.datanodeID, err)
			time.Sleep(2 * time.Second)
			continue
		}

		if resp.PuedeIniciar {
			log.Printf("Datanode %s: ¡Puedo iniciar! %s", s.datanodeID, resp.Mensaje)
			break
		}

		time.Sleep(3 * time.Second)
	}
}

func main() {
	datanodePtr := flag.String("nodo", "DN1", "ID del datanode (DN1, DN2, DN3)")
	flag.Parse()

	var puerto string
	var knownNodes []string
	
	switch *datanodePtr {
	case "DN1": 
		puerto = "50058"
		datanode2Host := os.Getenv("DATANODE2_HOST")
		datanode3Host := os.Getenv("DATANODE3_HOST")
		if datanode2Host == "" { datanode2Host = "localhost" }
		if datanode3Host == "" { datanode3Host = "localhost" }
		knownNodes = []string{datanode2Host + ":50059", datanode3Host + ":50060"}
	case "DN2": 
		puerto = "50059"
		datanode1Host := os.Getenv("DATANODE1_HOST")
		datanode3Host := os.Getenv("DATANODE3_HOST")
		if datanode1Host == "" { datanode1Host = "localhost" }
		if datanode3Host == "" { datanode3Host = "localhost" }
		knownNodes = []string{datanode1Host + ":50058", datanode3Host + ":50060"}
	case "DN3": 
		puerto = "50060"
		datanode1Host := os.Getenv("DATANODE1_HOST")
		datanode2Host := os.Getenv("DATANODE2_HOST")
		if datanode1Host == "" { datanode1Host = "localhost" }
		if datanode2Host == "" { datanode2Host = "localhost" }
		knownNodes = []string{datanode1Host + ":50058", datanode2Host + ":50059"}
	default: 
		puerto = "50058"
		knownNodes = []string{}
	}

	vuelos := cargarFlightUpdates()
	
	brokerHost := os.Getenv("BROKER_HOST")
	if brokerHost == "" {
		brokerHost = "localhost"
	}
	brokerConn := conectarConBroker(brokerHost + ":50051")


	datanode := &datanodeServer{
		datanodeID:       *datanodePtr,
		puerto:           puerto,
		brokerConn:       brokerConn,
		vuelosDisponibles: vuelos,
		estadoVuelos:     make(map[string]*EstadoVuelo),
		reservas:         make(map[string]*Reserva),
		knownNodes:       knownNodes,
	}

	ctx := context.Background()

	datanodeHost := os.Getenv("DATANODE_HOST")
	if datanodeHost == "" {
		datanodeHost = "localhost"
	}

	_, err := brokerConn.RegistrarEntidad(ctx, &pb.RegistroRequest{
		TipoEntidad: "datanode",
		IdEntidad:   *datanodePtr,
		Direccion:   datanodeHost + ":" + puerto,
	})
	if err != nil {
		log.Fatalf("Error registrando datanode en broker: %v", err)
	}

	datanode.esperarInicio()
	datanode.inicializarRelojVectorial()
	datanode.inicializarEstado()
	datanode.iniciarGossip()

	lis, err := net.Listen("tcp", ":"+puerto)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterAeroDistServer(s, datanode)

	log.Printf("Datanode %s iniciado en puerto %s", *datanodePtr, puerto)
	log.Printf("Vuelos cargados: %d", len(vuelos))
	log.Printf("Gossip configurado con %d nodos", len(knownNodes))

	go func() {
		for {
			resp, err := brokerConn.SolicitarInicio(ctx, &pb.InicioRequest{
				TipoEntidad: "datanode",
				IdEntidad:   *datanodePtr,
			})
			if err != nil {
				log.Printf("Error solicitando inicio: %v", err)
				time.Sleep(2 * time.Second)
				continue
			}

			if resp.PuedeIniciar {
				log.Printf("Datanode %s autorizado para operar", *datanodePtr)
				break
			}
			time.Sleep(3 * time.Second)
		}
	}()

	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}