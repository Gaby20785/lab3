package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"sync"
	"time"
	"os"

	"google.golang.org/grpc"

	pb "lab3/coordinador/proto"
)

// Estructura del coordinador que gestiona las sesiones (sticky session) para garantizar
// el Read Your Writes
type coordinadorServer struct {
	pb.UnimplementedAeroDistServer
	mu                    sync.RWMutex
	clientesRYWConectados map[string]bool
	brokerConn            pb.AeroDistClient
	coordinadorID         string
	registradoEnBroker    bool
	sesiones              map[string]*Sesion
}

// Sticky Session entre un cliente y un Datanode, tiene los datos del cliente, cuando se conectó
// por última vez y el ttl para que gestione la expiración de sesiones
type Sesion struct {
	datanodeAddress string
	lastAccess      time.Time
	clienteID       string
	ttl             time.Duration
}

// obtenerSesionValida - Verifica si una sesión existe para un cliente en particular, si no existe
// o se terminó su ttl retorna false
func (s *coordinadorServer) obtenerSesionValida(clienteID string) (*Sesion, bool) {
    s.mu.Lock()
    defer s.mu.Unlock()

    sesion, exists := s.sesiones[clienteID]
    if !exists {
        return nil, false
    }

    if time.Since(sesion.lastAccess) > sesion.ttl {
        delete(s.sesiones, clienteID)
        log.Printf("Sesión expirada eliminada inmediatamente para %s", clienteID)
        return nil, false
    }
    return sesion, true
}

// actualizarLastAccess - Cada vez que un cliente realiza una operación se actualiza el lastAccess
// para que no se expire la sesión
func (s *coordinadorServer) actualizarLastAccess(clienteID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if sesion, exists := s.sesiones[clienteID]; exists {
		sesion.lastAccess = time.Now()
	}
}

// limpiarSesionesExpiradas - Cada cierto tiempo revisa si hay sesiones expiradas, de ser así las elimina
func (s *coordinadorServer) limpiarSesionesExpiradas() {
	ticker := time.NewTicker(30 * time.Second)
	go func() {
		for range ticker.C {
			s.mu.Lock()
			expiradas := 0
			for clientID, sesion := range s.sesiones {
				if time.Since(sesion.lastAccess) > sesion.ttl {
					delete(s.sesiones, clientID)
					expiradas++
					log.Printf("Sesión expirada eliminada para cliente %s", clientID)
				}
			}
			if expiradas > 0 {
				log.Printf("Limpieza completada: %d sesiones expiradas eliminadas", expiradas)
			}
			s.mu.Unlock()
		}
	}()
}

// RegistrarEntidad - Registra a los clientes RYW y el coordinador se registra con el Broker
// si todos los clientes RYW se registraron
func (s *coordinadorServer) RegistrarEntidad(ctx context.Context, req *pb.RegistroRequest) (*pb.RegistroResponse, error) {
	s.mu.Lock()
	s.clientesRYWConectados[req.IdEntidad] = true
	s.mu.Unlock()

	log.Printf("Cliente RYW registrado: %s - Total: %d/3", req.IdEntidad, len(s.clientesRYWConectados))

	if len(s.clientesRYWConectados) == 3 && !s.registradoEnBroker {
		s.registrarEnBroker()
	}

	return &pb.RegistroResponse{
		Exito:   true,
		Mensaje: fmt.Sprintf("Coordinador: %s registrado", req.IdEntidad),
	}, nil
}

// SolicitarInicio - Solicita iniciar al Broker si todos los clientes RYW están registrados con el
// coordinador, si faltan clientes se dice que aún no se registra el coordinador con el Broker
func (s *coordinadorServer) SolicitarInicio(ctx context.Context, req *pb.InicioRequest) (*pb.InicioResponse, error) {
	if s.registradoEnBroker {
		resp, err := s.brokerConn.SolicitarInicio(ctx, &pb.InicioRequest{
			TipoEntidad: "coordinador",
			IdEntidad:   s.coordinadorID,
		})
		if err != nil {
			return nil, err
		}
		return resp, nil
	}
	return &pb.InicioResponse{PuedeIniciar: false, Mensaje: "Coordinador no registrado en broker aún"}, nil
}

// ObtenerEstadoInicial - Cuando un cliente quiere solicitar el estado de un vuelo, el coordinador
// la consulta al Broker con el Datanode asignado al cliente, en caso de no tener Datanode asignado
// se asigna uno por Round Robin
func (s *coordinadorServer) ObtenerEstadoInicial(ctx context.Context, req *pb.EstadoInicialRequest) (*pb.EstadoInicialResponse, error) {
	log.Printf("Coordinador redirige ObtenerEstadoInicial del cliente %s", req.ClienteId)

	sesion, sesionValida := s.obtenerSesionValida(req.ClienteId)

	stickyInfo := &pb.StickyInfo{
		ClienteId: req.ClienteId,
	}

	if sesionValida {
		stickyInfo.TieneSesion = true
		stickyInfo.DatanodeAddress = sesion.datanodeAddress
		log.Printf("Coordinador: Enviando sticky session %s para cliente %s",
			sesion.datanodeAddress, req.ClienteId)
		
		s.actualizarLastAccess(req.ClienteId)
	} else {
		log.Printf("Coordinador: Cliente %s sin sesión válida, broker asignará datanode", req.ClienteId)
	}

	requestConSticky := &pb.EstadoInicialRequest{
		ClienteId:  req.ClienteId,
		VueloId:    req.VueloId,
		StickyInfo: stickyInfo,
	}

	resp, err := s.brokerConn.ObtenerEstadoInicial(ctx, requestConSticky)
	if err != nil {
		log.Printf("Error redirigiendo al broker: %v", err)
		return nil, err
	}

	return resp, nil
}

// RealizarCheckIn - Cuando un cliente quiere hacer un CheckIn el coordinador lo redirige al coordinador
// para que consulte al Datanode asignado a este cliente, para garantizar la consistencia de la lectura
func (s *coordinadorServer) RealizarCheckIn(ctx context.Context, req *pb.CheckInRequest) (*pb.CheckInResponse, error) {
	log.Printf("Coordinador redirige RealizarCheckIn del cliente %s", req.ClienteId)

	sesion, sesionValida := s.obtenerSesionValida(req.ClienteId)

	stickyInfo := &pb.StickyInfo{
		ClienteId: req.ClienteId,
	}

	if sesionValida {
		stickyInfo.TieneSesion = true
		stickyInfo.DatanodeAddress = sesion.datanodeAddress
		log.Printf("Coordinador: Cliente %s tiene sesión con datanode %s",
			req.ClienteId, sesion.datanodeAddress)
	} else {
		log.Printf("Coordinador: Cliente %s sin sesión válida, broker asignará datanode", req.ClienteId)
	}

	requestConSticky := &pb.CheckInRequest{
		ClienteId:  req.ClienteId,
		VueloId:    req.VueloId,
		Asiento:    req.Asiento,
		RequestId:  req.RequestId,
		StickyInfo: stickyInfo,
	}

	resp, err := s.brokerConn.RealizarCheckIn(ctx, requestConSticky)
	if err != nil {
		log.Printf("Error redirigiendo al broker: %v", err)
		return nil, err
	}

	if resp.Exito && resp.Asignacion != nil {
		s.mu.Lock()
		s.sesiones[req.ClienteId] = &Sesion{
			datanodeAddress: resp.Asignacion.DatanodeAddress,
			lastAccess:      time.Now(),
			clienteID:       req.ClienteId,
			ttl:             30 * time.Second,
		}
		s.mu.Unlock()
		log.Printf("Coordinador: sesión creada - cliente %s -> datanode %s",
			req.ClienteId, resp.Asignacion.DatanodeAddress)
	} else if sesionValida {
		s.actualizarLastAccess(req.ClienteId)
	}

	return resp, nil
}

// ObtenerTarjetaEmbarque - Cuando un cliente quiere leer inmediatamente despues de escribir, el coordinador
// reenvía la solicitud al Broker para que consulte al Datanode asociado al cliente, como es donde se escribió
// cumple con leer lo que escribes
func (s *coordinadorServer) ObtenerTarjetaEmbarque(ctx context.Context, req *pb.TarjetaRequest) (*pb.TarjetaResponse, error) {
	log.Printf("Coordinador procesa ObtenerTarjetaEmbarque del cliente %s", req.ClienteId)

	sesion, sesionValida := s.obtenerSesionValida(req.ClienteId)

	stickyInfo := &pb.StickyInfo{
		ClienteId: req.ClienteId,
	}

	if sesionValida {
		stickyInfo.TieneSesion = true
		stickyInfo.DatanodeAddress = sesion.datanodeAddress
		log.Printf("Sticky session: cliente %s -> datanode %s", req.ClienteId, sesion.datanodeAddress)
		s.actualizarLastAccess(req.ClienteId)
	} else {
		stickyInfo.TieneSesion = false
		log.Printf("Coordinador: Cliente %s no tiene sesión activa", req.ClienteId)
	}

	requestConSticky := &pb.TarjetaRequest{
		ClienteId:          req.ClienteId,
		TarjetaEmbarqueId: req.TarjetaEmbarqueId,
		StickyInfo:        stickyInfo,
	}

	resp, err := s.brokerConn.ObtenerTarjetaEmbarque(ctx, requestConSticky)
	if err != nil {
		log.Printf("Error redirigiendo al broker: %v", err)
		return nil, err
	}

	return resp, nil
}

// registrarEnBroker - El coordinador se registra en el Broker
func (s *coordinadorServer) registrarEnBroker() {
	ctx := context.Background()

	coordinadorHost := os.Getenv("COORDINADOR_HOST")
	if coordinadorHost == "" {
		coordinadorHost = "localhost"
	}
	direccionCoordinador := coordinadorHost + ":50052"

	_, err := s.brokerConn.RegistrarEntidad(ctx, &pb.RegistroRequest{
		TipoEntidad: "coordinador",
		IdEntidad:   s.coordinadorID,
		Direccion:   direccionCoordinador,
	})
	if err != nil {
		log.Printf("Error registrando coordinador en broker: %v", err)
		return
	}

	s.registradoEnBroker = true
	log.Printf("Coordinador registrado en broker - Clientes RYW completos: 3/3")
}

// conectarConBroker - El coordinador se conecta con el Broker
func conectarConBroker(address string) pb.AeroDistClient {
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("No se pudo conectar con broker: %v", err)
	}
	return pb.NewAeroDistClient(conn)
}

// main - Inicializa al coordinador, se conecta con el Broker, se pone a limpiar sesiones de fondo,
// empieza a escuchar en el puerto 50052 para solicitar inicio al Broker hasta que inicia con la autorización
// del Broker
func main() {
	coordinadorID := flag.String("coordinador", "C1", "ID del coordinador")
	flag.Parse()

	brokerHost := os.Getenv("BROKER_HOST")
	if brokerHost == "" {
		brokerHost = "localhost"
	}
	brokerConn := conectarConBroker(brokerHost + ":50051")

	coordinador := &coordinadorServer{
		clientesRYWConectados: make(map[string]bool),
		brokerConn:            brokerConn,
		coordinadorID:         *coordinadorID,
		registradoEnBroker:    false,
		sesiones:              make(map[string]*Sesion),
	}

	coordinador.limpiarSesionesExpiradas()

	lis, err := net.Listen("tcp", ":50052")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterAeroDistServer(s, coordinador)

	log.Printf("Coordinador %s iniciado (solo sesiones) en puerto 50052", *coordinadorID)
	log.Printf("Esperando 3 clientes RYW antes de registrar en broker...")

	go func() {
		for {
			if coordinador.registradoEnBroker {
				ctx := context.Background()
				resp, err := brokerConn.SolicitarInicio(ctx, &pb.InicioRequest{
					TipoEntidad: "coordinador",
					IdEntidad:   *coordinadorID,
				})
				if err != nil {
					log.Printf("Error solicitando inicio: %v", err)
					time.Sleep(2 * time.Second)
					continue
				}

				if resp.PuedeIniciar {
					log.Printf("Autorización recibida! Coordinador listo")
					break
				}
			}
			time.Sleep(3 * time.Second)
		}
	}()

	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}

}
