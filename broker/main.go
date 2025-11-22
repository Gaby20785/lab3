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
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc"

	pb "lab3/broker/proto"
)

type brokerServer struct {
	pb.UnimplementedAeroDistServer
	mu sync.RWMutex
	entidadesConectadas map[string]string
	todosListos         bool
	vuelosDisponibles   []string
	datanodes           []string
	nextDatanode        int
	clientDatanodeMap   map[string]string
	vectorClock         map[string]int32
}

type FlightUpdate struct {
	SimTimeSec  int64
	FlightID    string
	UpdateType  string
	UpdateValue string
}

func (s *brokerServer) cargarVuelos() {
	s.vuelosDisponibles = cargarFlightUpdates()
	log.Printf("Broker: Lista de vuelos cargada - %d vuelos únicos", len(s.vuelosDisponibles))
}

func (s *brokerServer) iniciarSimulacion() {
	s.esperarConexion()
	
	log.Printf("BROKER: Iniciando lectura de eventos...")
	
	s.asignarPistasIniciales()

	events := s.cargarEventos()
	s.enviarEventos(events)
}

func (s *brokerServer) asignarPistasIniciales() {
    log.Printf("Broker: Asignando pistas iniciales a %d vuelos", len(s.vuelosDisponibles))
    
    for i, vueloID := range s.vuelosDisponibles {
        if i >= 20 {
            log.Printf("broker: Más de 20 vuelos, no hay pistas para %s", vueloID)
            continue
        }
        
        pistaSolicitada := fmt.Sprintf("PISTA_%02d", i+1)
        log.Printf("Broker: Asignando %s a %s", pistaSolicitada, vueloID)
        s.asignarPistaConsenso(vueloID, pistaSolicitada)
    }
}

func (s *brokerServer) procesarOperacionPista(update FlightUpdate) {
    if update.UpdateType == "puerta" {
        log.Printf("Broker: Vuelo %s cambió a puerta %s - reasignando pista", 
            update.FlightID, update.UpdateValue)
        
        pistaAleatoria := fmt.Sprintf("PISTA_%02d", rand.Intn(20)+1)
		vueloID := update.FlightID
		s.liberarPistaConsenso(vueloID)
    	s.asignarPistaConsenso(vueloID, pistaAleatoria)
    }
}

func (s *brokerServer) asignarPistaConsenso(vueloID, pistaSolicitada string) {
    log.Printf("Broker: Solicitando pista %s para %s", pistaSolicitada, vueloID)
    
    nodos := []string{"localhost:50061", "localhost:50062", "localhost:50063"}
    intento := 0
    
    for _, nodoAddr := range nodos {
        pista, exito, pistaOcupada := s.enviarAsignacionPista(nodoAddr, vueloID, pistaSolicitada)
        
        if exito {
            log.Printf("Broker: Pista %s asignada a %s", pista, vueloID)
            return
        }
        
        if pistaOcupada {
            log.Printf("Broker: Pista %s ocupada, intento %d/%d", pistaSolicitada, intento+1)
            pistaSolicitada = fmt.Sprintf("PISTA_%02d", rand.Intn(20)+1)
            break
        }
    }
    intento++
    time.Sleep(100 * time.Millisecond)
    
}

func (s *brokerServer) liberarPistaConsenso(vueloID string) {
    nodos := []string{"localhost:50061", "localhost:50062", "localhost:50063"}
    
    for _, nodoAddr := range nodos {
        exito := s.enviarLiberarPista(nodoAddr, vueloID)
        if exito {
            log.Printf("Broker: Pista liberada de %s", vueloID)
            return
        }
    }
    
    log.Printf("Broker: No se pudo liberar pista de %s", vueloID)
}

func (s *brokerServer) enviarAsignacionPista(nodoAddr, vueloID, pistaSolicitada string) (string, bool, bool) {
    conn, err := grpc.Dial(nodoAddr, grpc.WithInsecure(), grpc.WithTimeout(3*time.Second))
    if err != nil {
        return "", false, false
    }
    defer conn.Close()

    client := pb.NewAeroDistClient(conn)
    ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
    defer cancel()

    resp, err := client.AsignarPista(ctx, &pb.SolicitudPista{
        VueloId:         vueloID,
        PistaSolicitada: pistaSolicitada,
    })

    if err != nil {
        return "", false, false
    }

    if resp.Redirigir && resp.LiderActual != "" {
        liderAddr := "localhost:" + resp.LiderActual
        return s.enviarAsignacionPista(liderAddr, vueloID, pistaSolicitada)
    }

    return resp.PistaAsignada, resp.Exito, resp.PistaOcupada
}

func (s *brokerServer) enviarLiberarPista(nodoAddr, vueloID string) bool {
    conn, err := grpc.Dial(nodoAddr, grpc.WithInsecure(), grpc.WithTimeout(3*time.Second))
    if err != nil {
        return false
    }
    defer conn.Close()

    client := pb.NewAeroDistClient(conn)
    ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
    defer cancel()

    resp, err := client.LiberarPista(ctx, &pb.SolicitudLiberar{
        VueloId: vueloID,
        PistaId: "CUALQUIERA",
    })

    if err != nil {
        return false
    }

    if resp.Redirigir && resp.LiderActual != "" {
        liderAddr := "localhost:" + resp.LiderActual
        return s.enviarLiberarPista(liderAddr, vueloID)
    }

    return resp.Exito
}

func (s *brokerServer) esperarConexion() {
	log.Printf("Broker: Esperando conexión de todas las entidades...")
	
	for {
		s.mu.RLock()
		totalConectadas := len(s.entidadesConectadas)
		s.mu.RUnlock()

		totalEsperado := 9
		
		if totalConectadas >= totalEsperado {
			log.Printf("Broker: Todas las %d entidades conectadas!", totalEsperado)
			s.mu.Lock()
			s.todosListos = true
			s.mu.Unlock()
			break
		}
		time.Sleep(3 * time.Second)
	}
}

func (s *brokerServer) cargarEventos() []FlightUpdate {
	file, err := os.Open("flight_updates.csv")
	if err != nil {
		log.Printf("No se pudo abrir flight_updates.csv: %v", err)
		return []FlightUpdate{}
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		log.Printf("Error leyendo CSV: %v", err)
		return []FlightUpdate{}
	}

	var events []FlightUpdate
	
	for i, record := range records {
		if i == 0 {
			continue
		}
		
		if len(record) >= 4 {
			simTime, _ := strconv.ParseInt(record[0], 10, 64)
			flightID := record[1]
			updateType := record[2]
			updateValue := record[3]
			
			events = append(events, FlightUpdate{
				SimTimeSec:  simTime,
				FlightID:    flightID,
				UpdateType:  updateType,
				UpdateValue: updateValue,
			})
		}
	}

	log.Printf("Broker: Cargados %d eventos para simulación", len(events))
	return events
}

func (s *brokerServer) enviarEventos(events []FlightUpdate) {
	log.Printf("Broker: Iniciando envío de %d eventos", len(events))
	
	startTime := time.Now()
	var lastEventTime int64 = 0
	
	for i, event := range events {
		var pauseDuration time.Duration
		
		if i == 0 {
			pauseDuration = time.Duration(event.SimTimeSec) * time.Second
			if pauseDuration > 0 {
				time.Sleep(pauseDuration)
			}
		} else {
			pauseDuration = time.Duration(event.SimTimeSec - lastEventTime) * time.Second
			if pauseDuration > 0 {
				time.Sleep(pauseDuration)
			}
		}
		
		lastEventTime = event.SimTimeSec
		
		log.Printf("Broker: Ejecutando evento t=%ds", event.SimTimeSec)
		s.actualizarLosDatanodes(event)
		s.procesarOperacionPista(event)
	}
	
	totalTime := time.Since(startTime)
	log.Printf("Broker: Simulación completada! Tiempo total: %v", totalTime)
}

func (s *brokerServer) actualizarLosDatanodes(update FlightUpdate) {
	s.mu.RLock()
	datanodes := make([]string, len(s.datanodes))
	copy(datanodes, s.datanodes)
	s.mu.RUnlock()

	if len(datanodes) == 0 {
		return
	}

	s.mu.Lock()
	s.vectorClock["broker"]++
	relojActual := make(map[string]int32)
	for k, v := range s.vectorClock {
		relojActual[k] = v
	}
	s.mu.Unlock()

	relojVector := &pb.VectorClock{Clocks: relojActual}

	log.Printf("Broker: [t=%ds] Enviando [%s] %s=%s a %d datanodes", 
		update.SimTimeSec, update.FlightID, update.UpdateType, update.UpdateValue, len(datanodes))

	var wg sync.WaitGroup
	for _, datanodeAddr := range datanodes {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			s.actualizarDatanode(addr, update, relojVector)
		}(datanodeAddr)
	}
	wg.Wait()
}

func (s *brokerServer) actualizarDatanode(datanodeAddr string, update FlightUpdate, reloj *pb.VectorClock) {
	conn, err := grpc.Dial(datanodeAddr, grpc.WithInsecure())
	if err != nil {
		return
	}
	defer conn.Close()

	client := pb.NewAeroDistClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = client.EnviarActualizacionVuelo(ctx, &pb.ActualizacionVueloRequest{
		VueloId:     update.FlightID,
		UpdateType:  update.UpdateType,
		UpdateValue: update.UpdateValue,
		SimTimeSec:  update.SimTimeSec,
		RelojVector: reloj,
	})

	if err == nil {
		log.Printf("Evento [%s] entregado a %s", update.FlightID, datanodeAddr)
	}
}

func (s *brokerServer) RegistrarEntidad(ctx context.Context, req *pb.RegistroRequest) (*pb.RegistroResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.entidadesConectadas[req.IdEntidad] = req.TipoEntidad
	
	if req.TipoEntidad == "datanode" {
		s.datanodes = append(s.datanodes, req.Direccion)
	}
	
	log.Printf("Entidad registrada: %s (%s) - Total: %d", req.IdEntidad, req.TipoEntidad, len(s.entidadesConectadas))
	
	return &pb.RegistroResponse{
		Exito:   true,
		Mensaje: fmt.Sprintf("Broker: %s registrado", req.IdEntidad),
	}, nil
}

func (s *brokerServer) SolicitarInicio(ctx context.Context, req *pb.InicioRequest) (*pb.InicioResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.todosListos {
		return &pb.InicioResponse{PuedeIniciar: true, Mensaje: "Sistema listo"}, nil
	}

	totalEsperado := 9
	
	if len(s.entidadesConectadas) >= totalEsperado {
		s.mu.RUnlock()
		s.mu.Lock()
		s.todosListos = true
		s.mu.Unlock()
		s.mu.RLock()
		
		log.Printf("Todas las %d entidades conectadas! Iniciando sistema...", totalEsperado)
		return &pb.InicioResponse{PuedeIniciar: true, Mensaje: "Sistema listo"}, nil
	}

	return &pb.InicioResponse{
		PuedeIniciar: false,
		Mensaje:      fmt.Sprintf("Esperando... (%d/%d)", len(s.entidadesConectadas), totalEsperado),
	}, nil
}

func (s *brokerServer) ObtenerEstadoVuelo(ctx context.Context, req *pb.EstadoVueloRequest) (*pb.EstadoVueloResponse, error) {
	log.Printf("Broker: Cliente MR %s consulta estado del vuelo %s", req.ClienteId, req.VueloId)
	
	datanodeAddr := s.getNextDatanode()
	if datanodeAddr == "" {
		return &pb.EstadoVueloResponse{
			Exito:   false,
			Mensaje: "No hay datanodes disponibles",
		}, nil
	}
	
	conn, err := grpc.Dial(datanodeAddr, grpc.WithInsecure())
	if err != nil {
		return &pb.EstadoVueloResponse{
			Exito:   false,
			Mensaje: fmt.Sprintf("Error conectando al datanode: %v", err),
		}, nil
	}
	defer conn.Close()
	
	client := pb.NewAeroDistClient(conn)
	
	respDatanode, err := client.ObtenerEstadoVuelo(ctx, &pb.EstadoVueloRequest{
		VueloId:       req.VueloId,
		VersionCliente: req.VersionCliente,
		ClienteId:     req.ClienteId,
	})
	
	if err != nil {
		return &pb.EstadoVueloResponse{
			Exito:   false,
			Mensaje: fmt.Sprintf("Error consultando datanode: %v", err),
		}, nil
	}
	
	if !respDatanode.Exito {
		return &pb.EstadoVueloResponse{
			Exito:   false,
			Mensaje: respDatanode.Mensaje,
		}, nil
	}
	
	s.mu.Lock()
	s.vectorClock["broker"]++
	relojFinal := make(map[string]int32)
	for k, v := range s.vectorClock {
		relojFinal[k] = v
	}
	if respDatanode.VersionActual != nil {
		for k, v := range respDatanode.VersionActual.Clocks {
			if current, exists := relojFinal[k]; !exists || v > current {
				relojFinal[k] = v
			}
		}
	}
	s.mu.Unlock()
	
	return &pb.EstadoVueloResponse{
		Exito:         true,
		Mensaje:      respDatanode.Mensaje,
		VueloId:      respDatanode.VueloId,
		Estado:       respDatanode.Estado,
		Puerta:       respDatanode.Puerta,
		VersionActual: &pb.VectorClock{Clocks: relojFinal},
	}, nil
}

func (s *brokerServer) ObtenerEstadoInicial(ctx context.Context, req *pb.EstadoInicialRequest) (*pb.EstadoInicialResponse, error) {
	log.Printf("Broker procesa ObtenerEstadoInicial para cliente %s, vuelo %s", req.ClienteId, req.VueloId)
	
	datanodeAddr := s.getDatanodeForClient(req.ClienteId, req.StickyInfo)
	if datanodeAddr == "" {
		return &pb.EstadoInicialResponse{
			Exito:   false,
			Mensaje: "No hay datanodes disponibles",
		}, nil
	}
	
	conn, err := grpc.Dial(datanodeAddr, grpc.WithInsecure())
	if err != nil {
		return &pb.EstadoInicialResponse{
			Exito:   false,
			Mensaje: fmt.Sprintf("Error conectando al datanode: %v", err),
		}, nil
	}
	defer conn.Close()
	
	client := pb.NewAeroDistClient(conn)
	resp, err := client.ObtenerAsientosDisponibles(ctx, &pb.AsientosRequest{
		VueloId: req.VueloId,
	})
	
	if err != nil {
		return &pb.EstadoInicialResponse{
			Exito:   false,
			Mensaje: fmt.Sprintf("Error obteniendo asientos: %v", err),
		}, nil
	}
	
	if resp.Exito {
		var disponibles []string
		for asiento, ocupado := range resp.EstadoAsientos {
			if !ocupado {
				disponibles = append(disponibles, asiento)
			}
		}
		
		return &pb.EstadoInicialResponse{
			Exito:              true,
			Mensaje:           "Estado inicial obtenido",
			AsientosDisponibles: disponibles,
			TotalAsientos:      int32(len(resp.EstadoAsientos)),
			AsientosOcupados:   int32(len(resp.EstadoAsientos) - len(disponibles)),
		}, nil
	}
	
	return &pb.EstadoInicialResponse{
		Exito:   false,
		Mensaje: resp.Mensaje,
	}, nil
}

func (s *brokerServer) RealizarCheckIn(ctx context.Context, req *pb.CheckInRequest) (*pb.CheckInResponse, error) {
	log.Printf("Broker procesa RealizarCheckIn para cliente %s, vuelo %s, asiento %s", 
		req.ClienteId, req.VueloId, req.Asiento)
	
	datanodeAddr := s.getDatanodeForClient(req.ClienteId, req.StickyInfo)
	if datanodeAddr == "" {
		return &pb.CheckInResponse{
			Exito:   false,
			Mensaje: "No hay datanodes disponibles",
		}, nil
	}
	
	conn, err := grpc.Dial(datanodeAddr, grpc.WithInsecure())
	if err != nil {
		return &pb.CheckInResponse{
			Exito:   false,
			Mensaje: fmt.Sprintf("Error conectando al datanode: %v", err),
		}, nil
	}
	defer conn.Close()
	
	client := pb.NewAeroDistClient(conn)
	resp, err := client.ReservarAsiento(ctx, &pb.ReservaRequest{
		VueloId:   req.VueloId,
		Asiento:   req.Asiento,
		ClienteId: req.ClienteId,
		RequestId: req.RequestId,
	})
	
	if err != nil {
		return &pb.CheckInResponse{
			Exito:   false,
			Mensaje: fmt.Sprintf("Error reservando asiento: %v", err),
		}, nil
	}
	
	return &pb.CheckInResponse{
		Exito:             resp.Exito,
		Mensaje:           resp.Mensaje,
		TarjetaEmbarqueId: resp.TarjetaEmbarqueId,
		AsientoAsignado:   req.Asiento,
		Asignacion: &pb.DatanodeAsignacion{
			ClienteId:      req.ClienteId,
			DatanodeAddress: datanodeAddr,
		},
	}, nil
}

func (s *brokerServer) ObtenerTarjetaEmbarque(ctx context.Context, req *pb.TarjetaRequest) (*pb.TarjetaResponse, error) {
	log.Printf("Broker procesa ObtenerTarjetaEmbarque %s para cliente %s", 
		req.TarjetaEmbarqueId, req.ClienteId)
	
	datanodeAddr := s.getDatanodeForClient(req.ClienteId, req.StickyInfo)
	if datanodeAddr == "" {
		return &pb.TarjetaResponse{
			Exito:   false,
			Mensaje: "No hay datanodes disponibles",
		}, nil
	}
	
	conn, err := grpc.Dial(datanodeAddr, grpc.WithInsecure())
	if err != nil {
		return &pb.TarjetaResponse{
			Exito:   false,
			Mensaje: fmt.Sprintf("Error conectando al datanode: %v", err),
		}, nil
	}
	defer conn.Close()
	
	client := pb.NewAeroDistClient(conn)
	resp, err := client.ObtenerInformacionVuelo(ctx, &pb.InfoVueloRequest{
		TarjetaEmbarqueId: req.TarjetaEmbarqueId,
	})
	
	if err != nil {
		return &pb.TarjetaResponse{
			Exito:   false,
			Mensaje: fmt.Sprintf("Error obteniendo información: %v", err),
		}, nil
	}
	
	if resp.Exito {
		return &pb.TarjetaResponse{
			Exito:     true,
			Mensaje:   "Tarjeta obtenida",
			VueloId:   resp.VueloId,
			Asiento:   resp.Asiento,
			Estado:    resp.Estado,
			ClienteId: resp.ClienteId,
		}, nil
	}
	
	return &pb.TarjetaResponse{
		Exito:   false,
		Mensaje: resp.Mensaje,
	}, nil
}

func (s *brokerServer) EnviarActualizacionVuelo(ctx context.Context, req *pb.ActualizacionVueloRequest) (*pb.ActualizacionVueloResponse, error) {
	return &pb.ActualizacionVueloResponse{
		Exito:   false,
		Mensaje: "El broker no procesa actualizaciones, solo las distribuye",
	}, nil
}

func (s *brokerServer) getNextDatanode() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	if len(s.datanodes) == 0 {
		return ""
	}
	
	datanode := s.datanodes[s.nextDatanode]
	s.nextDatanode = (s.nextDatanode + 1) % len(s.datanodes)
	return datanode
}

func (s *brokerServer) getDatanodeForClient(clienteID string, stickyInfo *pb.StickyInfo) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	if stickyInfo != nil && stickyInfo.TieneSesion && stickyInfo.DatanodeAddress != "" {
		return stickyInfo.DatanodeAddress
	}
	
	if addr, exists := s.clientDatanodeMap[clienteID]; exists {
		return addr
	}
	
	if len(s.datanodes) == 0 {
		return ""
	}
	
	datanode := s.datanodes[s.nextDatanode]
	s.nextDatanode = (s.nextDatanode + 1) % len(s.datanodes)
	s.clientDatanodeMap[clienteID] = datanode
	
	return datanode
}

func cargarFlightUpdates() []string {
    file, err := os.Open("flight_updates.csv")
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

func main() {
	brokerID := flag.String("broker", "B1", "ID del broker")
	flag.Parse()

	broker := &brokerServer{
		entidadesConectadas: make(map[string]string),
		todosListos:         false,
		vuelosDisponibles:   []string{},
		datanodes:           make([]string, 0),
		clientDatanodeMap:   make(map[string]string),
		vectorClock:         make(map[string]int32),
	}

	broker.vectorClock["broker"] = 0
	broker.cargarVuelos()

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterAeroDistServer(s, broker)

	log.Printf("Broker %s iniciado en puerto 50051", *brokerID)
	log.Printf("Vuelos disponibles: %d", len(broker.vuelosDisponibles))
	
	go broker.iniciarSimulacion()
	
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}