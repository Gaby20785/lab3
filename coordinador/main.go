package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"

	pb "lab3/coordinador/proto"
)

type coordinadorServer struct {
	pb.UnimplementedAeroDistServer
	mu                    sync.RWMutex
	clientesRYWConectados map[string]bool
	brokerConn            pb.AeroDistClient
	coordinadorID         string
	registradoEnBroker    bool
	sesiones              map[string]*Sesion
}

type Sesion struct {
	datanodeAddress string
	lastAccess      time.Time
	clienteID       string
	ttl             time.Duration
}

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

func (s *coordinadorServer) actualizarLastAccess(clienteID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if sesion, exists := s.sesiones[clienteID]; exists {
		sesion.lastAccess = time.Now()
	}
}

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

func (s *coordinadorServer) registrarEnBroker() {
	ctx := context.Background()
	_, err := s.brokerConn.RegistrarEntidad(ctx, &pb.RegistroRequest{
		TipoEntidad: "coordinador",
		IdEntidad:   s.coordinadorID,
		Direccion:   "localhost:50052",
	})
	if err != nil {
		log.Printf("Error registrando coordinador en broker: %v", err)
		return
	}

	s.registradoEnBroker = true
	log.Printf("Coordinador registrado en broker - Clientes RYW completos: 3/3")
}

func conectarConBroker(address string) pb.AeroDistClient {
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("No se pudo conectar con broker: %v", err)
	}
	return pb.NewAeroDistClient(conn)
}

func main() {
	coordinadorID := flag.String("coordinador", "C1", "ID del coordinador")
	flag.Parse()

	brokerConn := conectarConBroker("localhost:50051")

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