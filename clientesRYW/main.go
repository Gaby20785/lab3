package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	"google.golang.org/grpc"

	pb "lab3/clientesRYW/proto"
)

// Estructura del Cliente RYW, este cliente recuerda lo último que escribió
// para poder confirmar que si lee seguido a escribir es consistente
type ryvClient struct {
	clientID          string
	coordinadorConn   pb.AeroDistClient
	vuelosDisponibles []string
	tarjetaEmbarqueID string
	ultimoAsiento     string
	ultimoVuelo       string
	estadoInicial     *pb.EstadoInicialResponse
}

// conectarConCoordinador - Se conecta con el coordinador
func (c *ryvClient) conectarConCoordinador(address string) {
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("No se pudo conectar con coordinador: %v", err)
	}
	c.coordinadorConn = pb.NewAeroDistClient(conn)
	log.Printf("Conectado al coordinador en %s", address)
}

// registrarEnCoordinador - Llama a RegistrarEntidad del coordinador para que guarde que tipo de entidad es
// y su dirección
func (c *ryvClient) registrarEnCoordinador() {
	ctx := context.Background()
	
	var puerto string
	switch c.clientID {
	case "RYW1": puerto = "50053"
	case "RYW2": puerto = "50054" 
	case "RYW3": puerto = "50055"
	default: puerto = "50053"
	}

	clienteHost := os.Getenv("CLIENTE_RYW_HOST")
	if clienteHost == "" {
		clienteHost = "localhost"
	}
	direccionCliente := clienteHost + ":" + puerto

	_, err := c.coordinadorConn.RegistrarEntidad(ctx, &pb.RegistroRequest{
		TipoEntidad: "cliente_ryw",
		IdEntidad:   c.clientID,
		Direccion:   direccionCliente,
	})
	if err != nil {
		log.Fatalf("Error registrando cliente en coordinador: %v", err)
	}
	
	log.Printf("Cliente %s registrado en coordinador", c.clientID)
}

// esperarInicio - Consulta al Coordinador continuamente si se puede iniciar la simulación,
// cuando se llega a la cantidad de entidades requeridas se inicia la simulación
func (c *ryvClient) esperarInicio() {
	ctx := context.Background()
	
	log.Printf("Cliente %s esperando autorización para iniciar...", c.clientID)
	for {
		resp, err := c.coordinadorConn.SolicitarInicio(ctx, &pb.InicioRequest{
			TipoEntidad: "cliente_ryw",
			IdEntidad:   c.clientID,
		})
		if err != nil {
			log.Printf("Error solicitando inicio: %v", err)
			time.Sleep(2 * time.Second)
			continue
		}

		if resp.PuedeIniciar {
			log.Printf("¡Puedo iniciar! %s", resp.Mensaje)
			break
		}

		time.Sleep(3 * time.Second)
	}
}

// solicitarEstadoInicial - Consulta los asientos disponibles de un vuelo al azar
func (c *ryvClient) solicitarEstadoInicial() bool {
	if len(c.vuelosDisponibles) == 0 {
		log.Printf("No hay vuelos disponibles")
		return false
	}

	vuelo := c.vuelosDisponibles[rand.Intn(len(c.vuelosDisponibles))]
	c.ultimoVuelo = vuelo

	ctx := context.Background()
	resp, err := c.coordinadorConn.ObtenerEstadoInicial(ctx, &pb.EstadoInicialRequest{
		ClienteId: c.clientID,
		VueloId:   vuelo,
	})

	if err != nil {
		log.Printf("Error obteniendo estado inicial: %v", err)
		return false
	}

	if resp.Exito {
		c.estadoInicial = resp
		log.Printf("Cliente %s - Estado inicial obtenido: Vuelo %s, Asientos disponibles: %d/%d", 
			c.clientID, vuelo, len(resp.AsientosDisponibles), resp.TotalAsientos)
		return true
	} else {
		log.Printf("Error al obtener estado inicial: %s", resp.Mensaje)
		return false
	}
}

// realizarCheckIn - Hace el CheckIn para reservar un asiento, si tiene éxito recuerda la tarjeta
// de embarque y el asiento que obtuvo
func (c *ryvClient) realizarCheckIn() bool {
	if c.estadoInicial == nil {
		if !c.solicitarEstadoInicial() {
			return false
		}
	}

	if len(c.estadoInicial.AsientosDisponibles) == 0 {
		log.Printf("No hay asientos disponibles en el vuelo %s", c.ultimoVuelo)
		return false
	}

	asiento := c.estadoInicial.AsientosDisponibles[rand.Intn(len(c.estadoInicial.AsientosDisponibles))]
	
	requestID := fmt.Sprintf("%s-%d", c.clientID, time.Now().UnixNano())

	ctx := context.Background()
	resp, err := c.coordinadorConn.RealizarCheckIn(ctx, &pb.CheckInRequest{
		ClienteId: c.clientID,
		VueloId:   c.ultimoVuelo,
		Asiento:   asiento,
		RequestId: requestID,
	})

	if err != nil {
		log.Printf("Error en check-in: %v", err)
		return false
	}

	if resp.Exito {
		log.Printf("Cliente %s - Check-in exitoso! Vuelo: %s, Asiento: %s", c.clientID, c.ultimoVuelo, asiento)
		c.tarjetaEmbarqueID = resp.TarjetaEmbarqueId
		c.ultimoAsiento = resp.AsientoAsignado
		return true
	} else {
		log.Printf("Cliente %s - Check-in fallido: %s", c.clientID, resp.Mensaje)
		c.estadoInicial = nil
		return false
	}
}

// obtenerTarjetaEmbarque - Consulta la tarjeta de embarque y compara los datos obtenidos con los que
// tiene en local para confirmar que está leyendo lo que escribió
func (c *ryvClient) obtenerTarjetaEmbarque() bool {
	if c.tarjetaEmbarqueID == "" {
		log.Printf("No hay tarjeta de embarque para consultar")
		return false
	}

	ctx := context.Background()
	resp, err := c.coordinadorConn.ObtenerTarjetaEmbarque(ctx, &pb.TarjetaRequest{
		ClienteId:          c.clientID,
		TarjetaEmbarqueId: c.tarjetaEmbarqueID,
	})

	if err != nil {
		log.Printf("Error obteniendo tarjeta: %v", err)
		return false
	}

	if resp.Exito {
		log.Printf("Cliente %s - Tarjeta obtenida: Vuelo: %s, Asiento: %s, Estado: %s", 
			c.clientID, resp.VueloId, resp.Asiento, resp.Estado)
		
		consistencia := true
		mensajes := []string{}
		
		if resp.VueloId == c.ultimoVuelo {
			mensajes = append(mensajes, "Vuelo coincide")
		} else {
			mensajes = append(mensajes, fmt.Sprintf("Vuelo: esperado %s, obtenido %s", c.ultimoVuelo, resp.VueloId))
			consistencia = false
		}
		
		if resp.Asiento == c.ultimoAsiento {
			mensajes = append(mensajes, "Asiento coincide")
		} else {
			mensajes = append(mensajes, fmt.Sprintf("Asiento: esperado %s, obtenido %s", c.ultimoAsiento, resp.Asiento))
			consistencia = false
		}
		
		if resp.ClienteId == c.clientID {
			mensajes = append(mensajes, "Cliente coincide")
		} else {
			mensajes = append(mensajes, fmt.Sprintf("Cliente: esperado %s, obtenido %s", c.clientID, resp.ClienteId))
			consistencia = false
		}
		
		if consistencia {
			log.Printf("Cliente %s - Consistencia verificada: %v", c.clientID, mensajes)
		} else {
			log.Printf("Cliente %s - Inconsistencia detectada: %v", c.clientID, mensajes)
		}
		return true
	} else {
		log.Printf("Error al obtener tarjeta: %s", resp.Mensaje)
		return false
	}
}

// ejecutarOperaciones - Consulta vuelos, realiza CheckIns y confirma el RYW constantemente con un tiempo
// de espera entre la sucesión de acciones anterior
func (c *ryvClient) ejecutarOperaciones() {
	log.Printf("Cliente RYW %s iniciando", c.clientID)
	time.Sleep(2 * time.Second)
	
	for {
		log.Printf("Cliente %s comenzando nuevo ciclo de check-in...", c.clientID)
		
		if c.solicitarEstadoInicial() {
			if c.realizarCheckIn() {
				time.Sleep(1 * time.Second)
				c.obtenerTarjetaEmbarque()
			}
		}
		
		espera := time.Duration(10 + rand.Intn(6)) * time.Second
		log.Printf("Cliente %s esperando %v para próximo check-in...", c.clientID, espera)
		time.Sleep(espera)
		
		c.estadoInicial = nil
		c.tarjetaEmbarqueID = ""
		c.ultimoAsiento = ""
		c.ultimoVuelo = ""
	}
}

// cargarFlightUpdates - Carga los IDs de los vuelos disponibles
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

// main - Inicializa al cliente, se conecta con el Coordinador, espera a que pueda iniciar y se pone a
// ejecutar la sucesión de operaciones de Consulta-CheckIn-Lectura-Verificación de RYW
func main() {
	clientPtr := flag.String("cliente", "RYW1", "ID del cliente (RYW1, RYW2, RYW3)")
	flag.Parse()

	vuelos := cargarFlightUpdates()

	client := &ryvClient{
		clientID:          *clientPtr,
		vuelosDisponibles: vuelos,
	}

	coordinadorHost := os.Getenv("COORDINADOR_HOST")
	if coordinadorHost == "" {
		coordinadorHost = "localhost"
	}

	client.conectarConCoordinador(coordinadorHost + ":50052")
	client.registrarEnCoordinador()
	client.esperarInicio()
	go client.ejecutarOperaciones()

	log.Printf("Cliente RYW %s completamente inicializado y operando", client.clientID)

	select {}

}
