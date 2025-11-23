package main

import (
	"context"
	"encoding/csv"
	"flag"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	"google.golang.org/grpc"

	pb "lab3/clientesMR/proto"
)

type mrClient struct {
	clientID      string
	brokerConn    pb.AeroDistClient
	vuelosDisponibles []string
	versionLocal  map[string]*pb.VectorClock
	mu            sync.RWMutex
}

func (c *mrClient) cargarFlightUpdates() []string {
	ruta := "flight_updates.csv"

	file, err := os.Open(ruta)
	if err != nil {
		log.Printf("Cliente MR %s: No se pudo abrir flight_updates.csv: %v", c.clientID, err)
		return []string{}
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		log.Printf("Cliente MR %s: Error leyendo CSV: %v", c.clientID, err)
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

	log.Printf("Cliente MR %s: Vuelos disponibles cargados- %d vuelos únicos", c.clientID, len(vuelos))
	return vuelos
}

func (c *mrClient) conectarConBroker(address string) {
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("Cliente MR %s: No se pudo conectar con broker: %v", c.clientID, err)
	}
	c.brokerConn = pb.NewAeroDistClient(conn)
	log.Printf("Cliente MR %s: Conectado al broker en %s", c.clientID, address)
}

func (c *mrClient) registrarEnBroker() {
	ctx := context.Background()
	
	var puerto string
	switch c.clientID {
	case "MR1":
		puerto = "50056"
	case "MR2":
		puerto = "50057"
	default:
		puerto = "50056"
	}

	clienteHost := os.Getenv("CLIENTE_MR_HOST")
	if clienteHost == "" {
		clienteHost = "localhost"
	}
	direccionCliente := clienteHost + ":" + puerto

	_, err := c.brokerConn.RegistrarEntidad(ctx, &pb.RegistroRequest{
		TipoEntidad: "cliente_mr",
		IdEntidad:   c.clientID,
		Direccion:   direccionCliente + puerto,
	})
	if err != nil {
		log.Fatalf("Cliente MR %s: Error registrando en broker: %v", c.clientID, err)
	}
	
	log.Printf("Cliente MR %s registrado en broker", c.clientID)
}

func (c *mrClient) esperarInicio() {
	ctx := context.Background()
	
	log.Printf("Cliente MR %s esperando autorización para iniciar...", c.clientID)
	for {
		resp, err := c.brokerConn.SolicitarInicio(ctx, &pb.InicioRequest{
			TipoEntidad: "cliente_mr",
			IdEntidad:   c.clientID,
		})
		if err != nil {
			log.Printf("Cliente MR %s: Error solicitando inicio: %v", c.clientID, err)
			time.Sleep(2 * time.Second)
			continue
		}

		if resp.PuedeIniciar {
			log.Printf("Cliente MR %s: ¡Puedo iniciar! %s", c.clientID, resp.Mensaje)
			break
		}

		time.Sleep(3 * time.Second)
	}
}

func (c *mrClient) consultarEstadoVuelo(vueloID string) bool {
	c.mu.RLock()
	versionCliente, tieneVersion := c.versionLocal[vueloID]
	c.mu.RUnlock()

	var versionEnviar *pb.VectorClock
	if tieneVersion {
		versionEnviar = versionCliente
		log.Printf("Cliente MR %s: Consultando vuelo %s con versión %v", c.clientID, vueloID, versionCliente.Clocks)
	} else {
		versionEnviar = &pb.VectorClock{Clocks: make(map[string]int32)}
		log.Printf("Cliente MR %s: Consultando vuelo %s (primera vez)", c.clientID, vueloID)
	}

	ctx := context.Background()
	resp, err := c.brokerConn.ObtenerEstadoVuelo(ctx, &pb.EstadoVueloRequest{
		VueloId:      vueloID,
		VersionCliente: versionEnviar,
		ClienteId:    c.clientID,
	})

	if err != nil {
		log.Printf("Cliente MR %s: Error consultando vuelo %s: %v", c.clientID, vueloID, err)
		return false
	}

	if resp.Exito {
		c.mu.Lock()
		if c.versionLocal == nil {
			c.versionLocal = make(map[string]*pb.VectorClock)
		}
		c.versionLocal[vueloID] = resp.VersionActual
		c.mu.Unlock()

		log.Printf("Cliente MR %s: Vuelo %s - Estado: %s, Puerta: %s", 
			c.clientID, vueloID, resp.Estado, resp.Puerta)
		log.Printf("Cliente MR %s: Versión actualizada -> %v", c.clientID, resp.VersionActual.Clocks)
		
		if tieneVersion {
			if c.verificarMonotonicidad(versionCliente, resp.VersionActual) {
				log.Printf("Cliente MR %s: Monotonic Reads verificado para %s", c.clientID, vueloID)
			} else {
				log.Printf("Cliente MR %s: No se cumple Monotonic Reads para %s", c.clientID, vueloID)
			}
		} else {
			log.Printf("Cliente MR %s: Primera consulta para %s - sin verificación MR", c.clientID, vueloID)
		}
		
		return true
	} else {
		log.Printf("Cliente MR %s: Error en consulta: %s", c.clientID, resp.Mensaje)
		return false
	}
}

func (c *mrClient) verificarMonotonicidad(viejo, nuevo *pb.VectorClock) bool {
    if viejo == nil || nuevo == nil {
        return true
    }
    
    violaciones := 0
    
    for nodo, valorViejo := range viejo.Clocks {
        if valorNuevo, existe := nuevo.Clocks[nodo]; existe {
            if valorNuevo < valorViejo {
                log.Printf("CLIENTE MR %s: VIOLACION Monotonic Reads - %s disminuyo: %d -> %d", 
                    c.clientID, nodo, valorViejo, valorNuevo)
                violaciones++
            }
        } else {
            log.Printf("CLIENTE MR %s: Advertencia - nodo %s desaparecio del reloj (posible recuperacion de fallos)", 
                c.clientID, nodo)
        }
    }
    
    for nodo := range nuevo.Clocks {
        if _, existe := viejo.Clocks[nodo]; !existe {
            log.Printf("CLIENTE MR %s: Nuevo nodo detectado: %s", c.clientID, nodo)
        }
    }
    
    return violaciones == 0
}

func (c *mrClient) ejecutarConsultas() {
	log.Printf("Cliente MR %s iniciando consultas continuas...", c.clientID)
	
	c.vuelosDisponibles = c.cargarFlightUpdates()
	
	log.Printf("Cliente MR %s: %d vuelos disponibles para consulta", c.clientID, len(c.vuelosDisponibles))
	
	consultasRealizadas := 0
	
	for {
		if len(c.vuelosDisponibles) == 0 {
			log.Printf("Cliente MR %s: No hay vuelos en lista maestra", c.clientID)
			time.Sleep(5 * time.Second)
			continue
		}
		
		vueloID := c.vuelosDisponibles[rand.Intn(len(c.vuelosDisponibles))]
		
		c.consultarEstadoVuelo(vueloID)
		
		consultasRealizadas++
		
		espera := time.Duration(3 + rand.Intn(6)) * time.Second
		log.Printf("Cliente MR %s: Esperando %v para próxima consulta...", c.clientID, espera)
		time.Sleep(espera)
	}
}

func main() {
	clientPtr := flag.String("cliente", "MR1", "ID del cliente (MR1, MR2)")
	flag.Parse()

	client := &mrClient{
		clientID:     *clientPtr,
		vuelosDisponibles: []string{},
		versionLocal: make(map[string]*pb.VectorClock),
	}
	brokerHost := os.Getenv("BROKER_HOST")
	if brokerHost == "" {
		brokerHost = "localhost"
	}
	client.conectarConBroker(brokerHost + ":50051")
	client.registrarEnBroker()
	client.esperarInicio()
	go client.ejecutarConsultas()

	log.Printf("Cliente MR %s completamente inicializado y operando", client.clientID)

	select {}
}