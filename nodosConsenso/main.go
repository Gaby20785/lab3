package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"

	pb "lab3/nodosConsenso/proto"
)

// Estructura de los nodos de consenso que deciden las acciones sobre partes criticas del sistema
type NodoConsenso struct {
	pb.UnimplementedAeroDistServer
	mu                      sync.RWMutex
	id                      string
	puerto                  string
	estado                  string
	terminoActual           int32
	votoPor                 string
	log                     []*pb.EntradaLog
	commitIndex             int32
	lastApplied             int32
	ultimoHeartbeat         time.Time
	timeoutEleccion         time.Duration
	pistas                  map[string]*EstadoPista
	brokerConn              pb.AeroDistClient
	registradoEnBroker      bool
	cacheLider              string
	ultimaVerificacionLider time.Time
	muCache                 sync.RWMutex
}

type EstadoPista struct {
	ID            string
	Ocupada       bool
	VueloAsignado string
	TipoUso       string
	UltimoUso     time.Time
}

// NuevoNodoConsenso - Construye un nodo de consenso
func NuevoNodoConsenso(id, puerto string) *NodoConsenso {
	timeoutBase := 1000 + rand.Intn(300)

	nodo := &NodoConsenso{
		id:                      id,
		puerto:                  puerto,
		estado:                  "SEGUIDOR",
		terminoActual:           0,
		votoPor:                 "",
		log:                     make([]*pb.EntradaLog, 0),
		commitIndex:             -1,
		lastApplied:             -1,
		pistas:                  make(map[string]*EstadoPista),
		timeoutEleccion:         time.Duration(timeoutBase) * time.Millisecond,
		registradoEnBroker:      false,
		cacheLider:              "",
		ultimaVerificacionLider: time.Time{},
	}

	nodo.inicializarPistas()
	log.Printf("%s: Inicializado en puerto %s", id, puerto)
	return nodo
}

// incializarPistas - Inicializa todas las pistas como vacías como estado inicial
func (n *NodoConsenso) inicializarPistas() {
	for i := 1; i <= 20; i++ {
		pistaID := fmt.Sprintf("PISTA_%02d", i)
		n.pistas[pistaID] = &EstadoPista{
			ID:      pistaID,
			Ocupada: false,
			TipoUso: "MIXTA",
		}
	}
	log.Printf("%s: %d pistas inicializadas", n.id, len(n.pistas))
}

// conectarConBroker - Se conecta con el Broker
func (n *NodoConsenso) conectarConBroker(address string) {
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("%s: No se pudo conectar con broker: %v", n.id, err)
	}
	n.brokerConn = pb.NewAeroDistClient(conn)
	log.Printf("%s: Conectado al broker en %s", n.id, address)
}

// registrarEnBroker - Se registra en el Broker como nodo de consenso
func (n *NodoConsenso) registrarEnBroker() {
	ctx := context.Background()

	consensoHost := os.Getenv("CONSENSO_HOST")
	if consensoHost == "" {
		consensoHost = "localhost"
	}
	direccionConsenso := consensoHost + ":" + n.puerto

	_, err := n.brokerConn.RegistrarEntidad(ctx, &pb.RegistroRequest{
		TipoEntidad: "nodo_consenso",
		IdEntidad:   n.id,
		Direccion:   direccionConsenso,
	})
	if err != nil {
		log.Fatalf("%s: Error registrando en broker: %v", n.id, err)
	}

	n.registradoEnBroker = true
	log.Printf("%s registrado en broker", n.id)
}

// esperarInicio - Solicita iniciar al Broker, solo incia si están todas las entidades requeridas para correr las simualción
func (n *NodoConsenso) esperarInicio() {
	ctx := context.Background()

	log.Printf("%s esperando autorización para iniciar...", n.id)
	for {
		resp, err := n.brokerConn.SolicitarInicio(ctx, &pb.InicioRequest{
			TipoEntidad: "nodo_consenso",
			IdEntidad:   n.id,
		})
		if err != nil {
			log.Printf("%s: Error solicitando inicio: %v", n.id, err)
			time.Sleep(2 * time.Second)
			continue
		}

		if resp.PuedeIniciar {
			log.Printf("%s: Puedo iniciar! %s", n.id, resp.Mensaje)
			break
		}

		log.Printf("%s: Esperando... %s", n.id, resp.Mensaje)
		time.Sleep(3 * time.Second)
	}
}

// bucleSeguidor - Lógica de un nodo seguidor, si el líder no ha dado señal de vida llama a elección para escoger un nuevo líder
func (n *NodoConsenso) bucleSeguidor() {
	for {
		n.mu.RLock()
		estado := n.estado
		ultimoHB := n.ultimoHeartbeat
		timeout := n.timeoutEleccion
		n.mu.RUnlock()

		if estado != "SEGUIDOR" {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		if time.Since(ultimoHB) > timeout {
			log.Printf("%s: Timeout de líder detectado, iniciando elección", n.id)
			n.iniciarEleccion()
		}

		time.Sleep(100 * time.Millisecond)
	}
}

// iniciarEleccion - Inicia una elección y cambia de seguidor a candidato votando por si mismo
// llamando a elección al resto de nodos
func (n *NodoConsenso) iniciarEleccion() {
	n.mu.Lock()

	if n.estado != "SEGUIDOR" {
		n.mu.Unlock()
		return
	}

	n.estado = "CANDIDATO"
	n.terminoActual++
	n.votoPor = n.id
	terminoEleccion := n.terminoActual
	n.ultimoHeartbeat = time.Now()

	n.mu.Unlock()

	log.Printf("%s: Iniciando elección para término %d", n.id, terminoEleccion)
	go n.ejecutarEleccion(terminoEleccion)
}

// ejecutarEleccion - Solicita los votos de los otros nodos contandolos para saber si gana o pierde la elección
func (n *NodoConsenso) ejecutarEleccion(terminoEleccion int32) {
	nodos := []string{"ATC1", "ATC2", "ATC3"}
	var wg sync.WaitGroup
	votosCanal := make(chan bool, len(nodos)-1)

	for _, nodoID := range nodos {
		if nodoID == n.id {
			continue
		}

		wg.Add(1)
		go func(objetivoID string) {
			defer wg.Done()
			if exito := n.solicitarVoto(objetivoID, terminoEleccion); exito {
				votosCanal <- true
			} else {
				votosCanal <- false
			}
		}(nodoID)
	}

	timeout := time.NewTimer(500 * time.Millisecond)
	defer timeout.Stop()

	votosCount := 1

	for i := 0; i < 2; i++ {
		select {
		case voto := <-votosCanal:
			if voto {
				votosCount++
				log.Printf("%s: Voto recibido - total: %d/3", n.id, votosCount)
				if votosCount > len(nodos)/2 {
					timeout.Stop()
					n.proclamarLider(terminoEleccion)
					wg.Wait()
					return
				}
			}
		case <-timeout.C:
			log.Printf("%s: Timeout en elección del término %d", n.id, terminoEleccion)
			n.finalizarEleccion(terminoEleccion, votosCount)
			wg.Wait()
			return
		}
	}

	wg.Wait()
	n.finalizarEleccion(terminoEleccion, votosCount)
}

// proclamarLider - Cambia del estado Candidato a Lider e inicia el heartbeat para dar señales de vida
func (n *NodoConsenso) proclamarLider(terminoEleccion int32) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.terminoActual != terminoEleccion || n.estado != "CANDIDATO" {
		return
	}

	n.estado = "LIDER"
	log.Printf("%s: ELEGIDO LIDER del término %d!", n.id, terminoEleccion)
	go n.bucleLider()
}

// finalizarEleccion - Gestiona los resultados de la elección tando si gana como si pierde
func (n *NodoConsenso) finalizarEleccion(terminoEleccion int32, votosRecibidos int) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.terminoActual == terminoEleccion && n.estado == "CANDIDATO" {
		if votosRecibidos > 1 {
			n.estado = "LIDER"
			log.Printf("%s: ELEGIDO LIDER del término %d! (%d/3 votos)",
				n.id, terminoEleccion, votosRecibidos)
			go n.bucleLider()
		} else {
			n.estado = "SEGUIDOR"
			log.Printf("%s: Elección perdida (%d/3 votos)", n.id, votosRecibidos)
		}
	}
}

// solicitarVoto - Solicita voto enviando su información a otro nodo
func (n *NodoConsenso) solicitarVoto(objetivoID string, termino int32) bool {
	n.mu.RLock()
	ultimoIndice := int32(len(n.log) - 1)
	var ultimoTermino int32 = 0
	if ultimoIndice >= 0 {
		ultimoTermino = n.log[ultimoIndice].Termino
	}
	n.mu.RUnlock()

	objetivoAddr := n.obtenerDireccionPorID(objetivoID)

	conn, err := grpc.Dial(objetivoAddr, grpc.WithInsecure(), grpc.WithTimeout(2*time.Second))
	if err != nil {
		log.Printf("%s: Error conectando a %s para solicitar voto", n.id, objetivoID)
		return false
	}
	defer conn.Close()

	client := pb.NewAeroDistClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resp, err := client.SolicitarVoto(ctx, &pb.SolicitudVoto{
		Termino:          termino,
		CandidatoId:      n.id,
		UltimoIndiceLog:  ultimoIndice,
		UltimoTerminoLog: ultimoTermino,
	})

	if err != nil {
		log.Printf("%s: Error solicitando voto a %s: %v", n.id, objetivoID, err)
		return false
	}

	return resp.VotoConcedido
}

// SolicitarVoto - Recibe la solicitur de voto y decide si votar por el nodo que envió la solicitud
func (n *NodoConsenso) SolicitarVoto(ctx context.Context, req *pb.SolicitudVoto) (*pb.RespuestaVoto, error) {
	log.Printf("%s: Recibiendo solicitud de voto de %s para término %d",
		n.id, req.CandidatoId, req.Termino)

	n.mu.Lock()
	defer n.mu.Unlock()

	if req.Termino > n.terminoActual {
		n.terminoActual = req.Termino
		n.estado = "SEGUIDOR"
		n.votoPor = ""
		log.Printf("%s: Actualizado a término %d, volviendo a SEGUIDOR", n.id, req.Termino)
	}

	votoConcedido := false

	if req.Termino >= n.terminoActual {
		if n.votoPor == "" || n.votoPor == req.CandidatoId {
			ultimoIndiceLocal := int32(len(n.log) - 1)
			var ultimoTerminoLocal int32 = 0
			if ultimoIndiceLocal >= 0 {
				ultimoTerminoLocal = n.log[ultimoIndiceLocal].Termino
			}

			logActualizado := (req.UltimoTerminoLog > ultimoTerminoLocal) ||
				(req.UltimoTerminoLog == ultimoTerminoLocal && req.UltimoIndiceLog >= ultimoIndiceLocal)

			if logActualizado {
				n.votoPor = req.CandidatoId
				votoConcedido = true
				log.Printf("%s: Voto concedido a %s para término %d", n.id, req.CandidatoId, req.Termino)
			} else {
				log.Printf("%s: Log no está actualizado, voto denegado", n.id)
			}
		}
	}

	return &pb.RespuestaVoto{
		Termino:       n.terminoActual,
		VotoConcedido: votoConcedido,
	}, nil
}

// AppendEntries - Verifica que el líder sea válido, verifica que el log anterior encaje con el nuevo
// y se añaden las nuevas entradas resolviendose posibles conflictos
func (n *NodoConsenso) AppendEntries(ctx context.Context, req *pb.SolicitudAppendEntries) (*pb.RespuestaAppendEntries, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.Termino < n.terminoActual {
		return &pb.RespuestaAppendEntries{
			Termino: n.terminoActual,
			Exito:   false,
		}, nil
	}

	if req.Termino >= n.terminoActual {
		if req.Termino > n.terminoActual {
			n.terminoActual = req.Termino
			n.votoPor = ""
			log.Printf("%s: Actualizado a término %d por líder %s", n.id, req.Termino, req.LiderId)
		}

		n.estado = "SEGUIDOR"
		n.ultimoHeartbeat = time.Now()
	}

	exito := true
	indiceCoincidente := int32(-1)

	if len(req.Entradas) > 0 {
		log.Printf("%s: Recibiendo %d entradas del líder %s", n.id, len(req.Entradas), req.LiderId)

		if req.IndicePrevio >= 0 {
			if req.IndicePrevio >= int32(len(n.log)) {
				exito = false
				log.Printf("%s: Índice previo %d fuera de rango (log size: %d)",
					n.id, req.IndicePrevio, len(n.log))
			} else if int32(len(n.log)) > 0 && n.log[req.IndicePrevio].Termino != req.TerminoPrevio {
				exito = false
				log.Printf("%s: Término previo no coincide: %d vs %d",
					n.id, n.log[req.IndicePrevio].Termino, req.TerminoPrevio)
			}
		} else if req.IndicePrevio == -1 {
			log.Printf("%s: Iniciando log desde el principio", n.id)
		}

		if exito {
			startIndex := int32(len(n.log))

			if req.IndicePrevio >= 0 && req.IndicePrevio < int32(len(n.log))-1 {
				n.log = n.log[:req.IndicePrevio+1]
				startIndex = req.IndicePrevio + 1
			}

			for i, entrada := range req.Entradas {
				nuevoIndice := startIndex + int32(i)
				entrada.Indice = nuevoIndice
				entrada.Termino = req.Termino
				n.log = append(n.log, entrada)
				log.Printf("%s: Entrada agregada [índice:%d, término:%d, comando:%s]",
					n.id, nuevoIndice, entrada.Termino, entrada.Comando)
			}
			indiceCoincidente = int32(len(n.log)) - 1
		}
	}

	if req.CommitIndex > n.commitIndex {
		nuevoCommitIndex := min(req.CommitIndex, int32(len(n.log)-1))
		if nuevoCommitIndex > n.commitIndex {
			log.Printf("%s: COMMIT avanzado de %d a %d", n.id, n.commitIndex, nuevoCommitIndex)
			n.commitIndex = nuevoCommitIndex
			n.aplicarCommits()
		}
	}

	return &pb.RespuestaAppendEntries{
		Termino:           n.terminoActual,
		Exito:             exito,
		IndiceCoincidente: indiceCoincidente,
	}, nil
}

// aplicarCommits - Avanza el estado aplicando las entradas del log desde el último guardado hasta
// el índice del commit
func (n *NodoConsenso) aplicarCommits() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		if n.lastApplied < int32(len(n.log)) {
			entrada := n.log[n.lastApplied]
			n.aplicarEntradaLog(entrada)
			log.Printf("%s: Aplicada entrada comprometida [indice:%d]", n.id, n.lastApplied)
		}
	}
}

// AsignarPista - Verifica si es el nodo líder y si la pista está desocupada, en caso de ser así llama a consenso
// en caso de llegar a un consenso favorable para la asignación de pista se asigna y se envía
func (n *NodoConsenso) AsignarPista(ctx context.Context, req *pb.SolicitudPista) (*pb.RespuestaPista, error) {
	n.mu.RLock()
	estado := n.estado
	n.mu.RUnlock()

	// SILENCIAR: No loguear si es una verificación de TEST
	if req.VueloId != "TEST" {
		log.Printf("%s: Líder procesando asignación de pista para vuelo %s", n.id, req.VueloId)
	}

	if estado != "LIDER" {
		return &pb.RespuestaPista{
			Exito:       false,
			Mensaje:     "No soy el líder",
			Redirigir:   true,
			LiderActual: n.descubrirLider(),
		}, nil
	}

	pistaSolicitada := req.PistaSolicitada
	pistaOcupada := false

	if pistaSolicitada != "" {
		n.mu.RLock()
		if pista, exists := n.pistas[pistaSolicitada]; exists && pista.Ocupada {
			pistaOcupada = true
			if req.VueloId != "TEST" { // Solo loguear si no es TEST
				log.Printf("%s: Pista %s OCUPADA por vuelo %s", n.id, pistaSolicitada, pista.VueloAsignado)
			}
		}
		n.mu.RUnlock()
	}

	if pistaOcupada {
		return &pb.RespuestaPista{
			Exito:        false,
			Mensaje:      "Pista ocupada",
			PistaOcupada: true,
		}, nil
	}

	// Para vuelos TEST, responder rápido sin replicación
	if req.VueloId == "TEST" {
		return &pb.RespuestaPista{
			Exito:         true,
			Mensaje:       "OK (test)",
			PistaAsignada: "TEST",
		}, nil
	}

	exito := n.replicarAsignacionPista(req.VueloId, pistaSolicitada)

	if exito {
		log.Printf("%s: ASIGNACION EXITOSA - Pista %s -> Vuelo %s", n.id, pistaSolicitada, req.VueloId)
		return &pb.RespuestaPista{
			Exito:         true,
			Mensaje:       "Pista asignada exitosamente",
			PistaAsignada: pistaSolicitada,
		}, nil
	} else {
		log.Printf("%s: FALLA EN ASIGNACION - No se alcanzó quórum", n.id)
		return &pb.RespuestaPista{
			Exito:   false,
			Mensaje: "Error de consenso: no se alcanzó quórum",
		}, nil
	}
}

// aplicarEntradaLog - Actualiza los estados de las pistas segun los comandos del log
func (n *NodoConsenso) aplicarEntradaLog(entrada *pb.EntradaLog) {
	log.Printf("%s: Aplicando entrada del log: %s", n.id, entrada.Comando)

	partes := strings.Split(entrada.Comando, ":")
	if len(partes) < 2 {
		log.Printf("%s: Comando inválido: %s", n.id, entrada.Comando)
		return
	}

	operacion := partes[0]
	vueloID := partes[1]

	switch operacion {
	case "ASIGNAR":
		if len(partes) >= 3 {
			pistaID := partes[2]
			n.aplicarAsignacionPista(vueloID, pistaID)
		}
	case "LIBERAR":
		n.aplicarLiberacionPista(vueloID)
	default:
		log.Printf("%s: Operación desconocida: %s", n.id, operacion)
	}
}

// LiberarPista - Gestiona la liberación de pistas con consenso
func (n *NodoConsenso) LiberarPista(ctx context.Context, req *pb.SolicitudLiberar) (*pb.RespuestaPista, error) {
	n.mu.RLock()
	estado := n.estado
	n.mu.RUnlock()

	if estado != "LIDER" {
		return &pb.RespuestaPista{
			Exito:       false,
			Mensaje:     "No soy el líder",
			Redirigir:   true,
			LiderActual: n.descubrirLider(),
		}, nil
	}

	exito := n.replicarLiberacionPista(req.VueloId)

	if exito {
		return &pb.RespuestaPista{
			Exito:   true,
			Mensaje: "Pista liberada exitosamente",
		}, nil
	} else {
		return &pb.RespuestaPista{
			Exito:   false,
			Mensaje: "Error de consenso: no se alcanzó quórum",
		}, nil
	}
}

// replicarAsignacionPista - Guarda el comando (asignar pista) en su log, llama a votación (sobre el consenso), si se cumple el quorum
// se hace oficial el cambio haciendo commit
func (n *NodoConsenso) replicarAsignacionPista(vueloID, pistaID string) bool {
	log.Printf("%s: Iniciando replicación para vuelo %s, pista %s", n.id, vueloID, pistaID)

	entrada := &pb.EntradaLog{
		Indice:  int32(len(n.log)),
		Termino: n.terminoActual,
		Comando: fmt.Sprintf("ASIGNAR:%s:%s", vueloID, pistaID),
	}

	n.mu.Lock()
	n.log = append(n.log, entrada)
	indiceLog := int32(len(n.log) - 1)
	n.mu.Unlock()

	log.Printf("%s: Entrada agregada al log local [indice:%d, termino:%d]", n.id, indiceLog, n.terminoActual)

	confirmaciones := 1
	log.Printf("%s: Replicando entrada %d a seguidores...", n.id, indiceLog)

	for _, seguidorID := range []string{"ATC1", "ATC2", "ATC3"} {
		if seguidorID == n.id {
			continue
		}

		if n.replicarEntradaSeguidor(seguidorID, indiceLog) {
			confirmaciones++
			log.Printf("%s: Seguidor %s CONFIRMO replica", n.id, seguidorID)
		} else {
			log.Printf("%s: Seguidor %s FALLO en confirmar", n.id, seguidorID)
		}
	}

	log.Printf("%s: Confirmaciones recibidas: %d/3", n.id, confirmaciones)

	if confirmaciones >= 2 {
		n.mu.Lock()
		n.commitIndex = indiceLog
		n.aplicarCommits()
		n.mu.Unlock()

		log.Printf("%s: Notificando COMMIT a seguidores...", n.id)
		n.notificarCommitASeguidores(indiceLog)

		log.Printf("%s: QUORUM ALCANZADO - Pista %s asignada a %s", n.id, pistaID, vueloID)
		return true
	}

	log.Printf("%s: QUORUM NO ALCANZADO - Solo %d/2 confirmaciones", n.id, confirmaciones)
	return false
}

// notificarCommitASeguidores - Envia el índice de commit actualizado a los seguidores para que puedan aplicarlos
func (n *NodoConsenso) notificarCommitASeguidores(commitIndex int32) {
	for _, seguidorID := range []string{"ATC1", "ATC2", "ATC3"} {
		if seguidorID == n.id {
			continue
		}
		go n.notificarCommitSeguidor(seguidorID, commitIndex)
	}
}

// notificarCommitSeguidor - Envía el índice de commit actualizado a un seguidor en particular para ser aplicados
func (n *NodoConsenso) notificarCommitSeguidor(seguidorID string, commitIndex int32) {
	seguidorAddr := n.obtenerDireccionPorID(seguidorID)

	conn, err := grpc.Dial(seguidorAddr, grpc.WithInsecure(), grpc.WithTimeout(2*time.Second))
	if err != nil {
		log.Printf("%s: Error conectando a %s para notificar commit", n.id, seguidorID)
		return
	}
	defer conn.Close()

	client := pb.NewAeroDistClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err = client.AppendEntries(ctx, &pb.SolicitudAppendEntries{
		Termino:     n.terminoActual,
		LiderId:     n.id,
		CommitIndex: commitIndex,
	})

	if err != nil {
		log.Printf("%s: Error notificando commit a %s", n.id, seguidorID)
	} else {
		log.Printf("%s: Commit notificado exitosamente a %s", n.id, seguidorID)
	}
}

// replicarLiberacionPista - Propone liberar cierta pista y llama a votación esta desición
// en caso de cumplir con el quorum necesario se hace commit de el comando y se libera la pista
func (n *NodoConsenso) replicarLiberacionPista(vueloID string) bool {
	entrada := &pb.EntradaLog{
		Indice:  int32(len(n.log)),
		Termino: n.terminoActual,
		Comando: fmt.Sprintf("LIBERAR:%s", vueloID),
	}

	n.mu.Lock()
	n.log = append(n.log, entrada)
	indiceLog := int32(len(n.log) - 1)
	n.mu.Unlock()

	confirmaciones := 1
	for _, seguidorID := range []string{"ATC1", "ATC2", "ATC3"} {
		if seguidorID == n.id {
			continue
		}
		if n.replicarEntradaSeguidor(seguidorID, indiceLog) {
			confirmaciones++
		}
	}

	if confirmaciones >= 2 {
		n.mu.Lock()
		n.commitIndex = indiceLog
		n.aplicarLiberacionPista(vueloID)
		n.mu.Unlock()

		log.Printf("%s: QUORUM - Pista liberada de %s", n.id, vueloID)
		return true
	}

	return false
}

// replicarEntradaSeguidor - Se conecta con un seguidor para enviarle una entrada, si el seguidor acepta
// confirma la consistencia del dato y retorna true
func (n *NodoConsenso) replicarEntradaSeguidor(seguidorID string, indice int32) bool {
	seguidorAddr := n.obtenerDireccionPorID(seguidorID)

	log.Printf("%s: Enviando entrada %d a seguidor %s...", n.id, indice, seguidorID)

	conn, err := grpc.Dial(seguidorAddr, grpc.WithInsecure(), grpc.WithTimeout(2*time.Second))
	if err != nil {
		log.Printf("%s: Error conectando a seguidor %s", n.id, seguidorID)
		return false
	}
	defer conn.Close()

	client := pb.NewAeroDistClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	n.mu.RLock()
	if indice < 0 || indice >= int32(len(n.log)) {
		n.mu.RUnlock()
		log.Printf("%s: Índice %d fuera de rango del log", n.id, indice)
		return false
	}
	entrada := n.log[indice]

	var indicePrevio int32 = -1
	var terminoPrevio int32 = 0

	if indice > 0 {
		indicePrevio = indice - 1
		terminoPrevio = n.log[indicePrevio].Termino
	}
	n.mu.RUnlock()

	resp, err := client.AppendEntries(ctx, &pb.SolicitudAppendEntries{
		Termino:       n.terminoActual,
		LiderId:       n.id,
		IndicePrevio:  indicePrevio,
		TerminoPrevio: terminoPrevio,
		Entradas:      []*pb.EntradaLog{entrada},
		CommitIndex:   n.commitIndex,
	})

	if err != nil {
		log.Printf("%s: Error replicando a %s: %v", n.id, seguidorID, err)
		return false
	}

	if !resp.Exito {
		log.Printf("%s: Seguidor %s rechazó la réplica", n.id, seguidorID)
		return false
	}

	return true
}

// aplicarAsignacionPista - Actualiza una pista a ocupada
func (n *NodoConsenso) aplicarAsignacionPista(vueloID, pistaID string) {
	if pista, exists := n.pistas[pistaID]; exists {
		if pista.Ocupada {
			log.Printf("%s: Pista %s ya estaba ocupada por %s", n.id, pistaID, pista.VueloAsignado)
		}
		pista.Ocupada = true
		pista.VueloAsignado = vueloID
		pista.TipoUso = "DESPEGUE"
		pista.UltimoUso = time.Now()
		log.Printf("%s: Pista %s asignada a vuelo %s", n.id, pistaID, vueloID)
	} else {
		log.Printf("%s: Pista %s no existe", n.id, pistaID)
	}
}

// aplicarLiberacionPista - Actualiza una pista a liberada
func (n *NodoConsenso) aplicarLiberacionPista(vueloID string) {
	liberada := false
	for pistaID, pista := range n.pistas {
		if pista.VueloAsignado == vueloID {
			pista.Ocupada = false
			pista.VueloAsignado = ""
			log.Printf("%s: Pista %s liberada del vuelo %s", n.id, pistaID, vueloID)
			liberada = true
			break
		}
	}

	if !liberada {
		log.Printf("%s: No se encontro pista asignada al vuelo %s", n.id, vueloID)
	}
}

// bucleLider - Envía hearbeats frecuentemente para dar señal de vida y seguir siendo líder
func (n *NodoConsenso) bucleLider() {
	log.Printf("%s: Iniciando bucle de líder para término %d", n.id, n.terminoActual)

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		n.mu.RLock()
		estado := n.estado
		n.mu.RUnlock()

		if estado != "LIDER" {
			return
		}

		n.enviarHeartbeats()
		<-ticker.C
	}
}

// enviarHeartbeats - Da señal de vida a los otros nodos de consenso
func (n *NodoConsenso) enviarHeartbeats() {
	for _, nodoID := range []string{"ATC1", "ATC2", "ATC3"} {
		if nodoID == n.id {
			continue
		}
		go n.enviarHeartbeat(nodoID)
	}
}

// enviarHeartbeat - Envia una señal de vida a un seguidor en específico
func (n *NodoConsenso) enviarHeartbeat(objetivoID string) {
	objetivoAddr := n.obtenerDireccionPorID(objetivoID)

	conn, err := grpc.Dial(objetivoAddr, grpc.WithInsecure(), grpc.WithTimeout(500*time.Millisecond))
	if err != nil {
		log.Printf("%s: Error conectando a %s para heartbeat", n.id, objetivoID)
		return
	}
	defer conn.Close()

	client := pb.NewAeroDistClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	_, err = client.AppendEntries(ctx, &pb.SolicitudAppendEntries{
		Termino: n.terminoActual,
		LiderId: n.id,
	})

	if err != nil {
		log.Printf("%s: Error enviando heartbeat a %s", n.id, objetivoID)
	}
}

// obtenerDireccionPorID - Obtiene la direccion de alguno de los nodos de consenso según su ID
func (n *NodoConsenso) obtenerDireccionPorID(id string) string {
	switch id {
	case "ATC1":
		host := os.Getenv("CONSENSO1_HOST")
		if host == "" {
			host = "localhost"
		}
		return host + ":50061"
	case "ATC2":
		host := os.Getenv("CONSENSO2_HOST")
		if host == "" {
			host = "localhost"
		}
		return host + ":50062"
	case "ATC3":
		host := os.Getenv("CONSENSO3_HOST")
		if host == "" {
			host = "localhost"
		}
		return host + ":50063"
	default:
		host := os.Getenv("CONSENSO1_HOST")
		if host == "" {
			host = "localhost"
		}
		return host + ":50061"
	}
}

// descubrirLider - Consulta a los otros nodos de consenso si alguno es el lider
func (n *NodoConsenso) descubrirLider() string {
	for _, id := range []string{"ATC1", "ATC2", "ATC3"} {
		if id == n.id {
			continue
		}

		if n.esLider(id) {
			return n.obtenerPuertoPorID(id)
		}
	}
	return "50061"
}

// esLider - Confirma si un nodo es lider
func (n *NodoConsenso) esLider(nodoID string) bool {
	// Cache simple para evitar verificaciones muy frecuentes
	n.mu.RLock()
	if time.Since(n.ultimaVerificacionLider) < 2*time.Second && n.cacheLider == nodoID {
		n.mu.RUnlock()
		return true
	}
	n.mu.RUnlock()

	addr := n.obtenerDireccionPorID(nodoID)
	conn, err := grpc.Dial(addr, grpc.WithInsecure(), grpc.WithTimeout(500*time.Millisecond))
	if err != nil {
		return false
	}
	defer conn.Close()

	client := pb.NewAeroDistClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	resp, err := client.AsignarPista(ctx, &pb.SolicitudPista{
		VueloId: "TEST",
	})

	if err == nil && !resp.Redirigir {
		// Actualizar cache
		n.mu.Lock()
		n.cacheLider = nodoID
		n.ultimaVerificacionLider = time.Now()
		n.mu.Unlock()
		return true
	}

	return false
}

// obtenerPuertoPorID - Obtiene el puerto de algún nodo de consenso
func (n *NodoConsenso) obtenerPuertoPorID(id string) string {
	switch id {
	case "ATC1":
		return "50061"
	case "ATC2":
		return "50062"
	case "ATC3":
		return "50063"
	default:
		return "50061"
	}
}

// min - Retorna el minimo entre dos números
func min(a, b int32) int32 {
	if a < b {
		return a
	}
	return b
}

// main - Inicializa un nodo de consenso, se conecta y registra con el Broker, espera a que de inicio al flujo de trabajo
// y comienza como nodo seguidor
func main() {
	nodoPtr := flag.String("nodo", "ATC1", "ID del nodo consenso (ATC1, ATC2, ATC3)")
	flag.Parse()

	var puerto string
	switch *nodoPtr {
	case "ATC1":
		puerto = "50061"
	case "ATC2":
		puerto = "50062"
	case "ATC3":
		puerto = "50063"
	default:
		puerto = "50061"
	}

	nodo := NuevoNodoConsenso(*nodoPtr, puerto)

	brokerHost := os.Getenv("BROKER_HOST")
	if brokerHost == "" {
		brokerHost = "localhost"
	}
	nodo.conectarConBroker(brokerHost + ":50051")

	nodo.registrarEnBroker()
	nodo.esperarInicio()

	lis, err := net.Listen("tcp", ":"+puerto)
	if err != nil {
		log.Fatalf("%s: Failed to listen: %v", *nodoPtr, err)
	}

	s := grpc.NewServer()
	pb.RegisterAeroDistServer(s, nodo)

	log.Printf("Nodo Consenso %s listo en puerto %s", *nodoPtr, puerto)

	go nodo.bucleSeguidor()

	if err := s.Serve(lis); err != nil {
		log.Fatalf("%s: Failed to serve: %v", *nodoPtr, err)
	}
}
