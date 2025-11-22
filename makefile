.PHONY: proto clean build all

# Generar código gRPC para todas las entidades
proto:
	mkdir -p broker/proto clientesMR/proto clientesRYW/proto coordinador/proto datanodes/proto nodosConsenso/proto
	protoc --go_out=./broker --go-grpc_out=./broker proto/aeroDist.proto
	protoc --go_out=./clientesMR --go-grpc_out=./clientesMR proto/aeroDist.proto
	protoc --go_out=./clientesRYW --go-grpc_out=./clientesRYW proto/aeroDist.proto
	protoc --go_out=./coordinador --go-grpc_out=./coordinador proto/aeroDist.proto
	protoc --go_out=./datanodes --go-grpc_out=./datanodes proto/aeroDist.proto
	protoc --go_out=./nodosConsenso --go-grpc_out=./nodosConsenso proto/aeroDist.proto

# Compilar todas las entidades
build: proto
	go build -o bin/broker ./broker
	go build -o bin/cliente-mr ./clientesMR
	go build -o bin/cliente-ryw ./clientesRYW
	go build -o bin/coordinador ./coordinador
	go build -o bin/datanode ./datanodes
	go build -o bin/consenso ./nodosConsenso

# Ejecutar entidades individuales
run-broker: build
	./bin/broker

run-coordinador: build
	./bin/coordinador

run-datanode1: build
	./bin/datanode --nodo=DN1

run-datanode2: build
	./bin/datanode --nodo=DN2

run-datanode3: build
	./bin/datanode --nodo=DN3

run-consenso1: build
	./bin/consenso --nodo=ATC1

run-consenso2: build
	./bin/consenso --nodo=ATC2

run-consenso3: build
	./bin/consenso --nodo=ATC3

run-cliente-mr1: build
	./bin/cliente-mr --cliente=MR1

run-cliente-mr2: build
	./bin/cliente-mr --cliente=MR2

run-cliente-ryw1: build
	./bin/cliente-ryw --cliente=RYW1

run-cliente-ryw2: build
	./bin/cliente-ryw --cliente=RYW2

run-cliente-ryw3: build
	./bin/cliente-ryw --cliente=RYW3

# Ejecutar múltiples instancias en una sola terminal
run-all-datanodes: build
	@echo "Iniciando todos los datanodes..."
	./bin/datanode --nodo=DN1 &
	./bin/datanode --nodo=DN2 &
	./bin/datanode --nodo=DN3 &
	@echo "Todos los datanodes iniciados en background"

run-all-consenso: build
	@echo "Iniciando todos los nodos de consenso..."
	./bin/consenso --nodo=ATC1 &
	./bin/consenso --nodo=ATC2 &
	./bin/consenso --nodo=ATC3 &
	@echo "Todos los nodos de consenso iniciados en background"

run-all-clientes-ryw: build
	@echo "Iniciando todos los clientes RYW..."
	./bin/cliente-ryw --cliente=RYW1 &
	./bin/cliente-ryw --cliente=RYW2 &
	./bin/cliente-ryw --cliente=RYW3 &
	@echo "Todos los clientes RYW iniciados en background"

run-all-clientes-mr: build
	@echo "Iniciando todos los clientes MR..."
	./bin/cliente-mr --cliente=MR1 &
	./bin/cliente-mr --cliente=MR2 &
	@echo "Todos los clientes MR iniciados en background"

# Ejecutar sistema completo paso a paso
run-sistema-completo: build
	@echo "🚀 Iniciando sistema AeroDist completo..."
	@echo "Paso 1/6: Iniciando Broker..."
	./bin/broker &
	@sleep 3
	@echo "Paso 2/6: Iniciando Coordinador..."
	./bin/coordinador &
	@sleep 2
	@echo "Paso 3/6: Iniciando Datanodes..."
	./bin/datanode --nodo=DN1 &
	./bin/datanode --nodo=DN2 &
	./bin/datanode --nodo=DN3 &
	@sleep 2
	@echo "Paso 4/6: Iniciando Nodos de Consenso..."
	./bin/consenso --nodo=ATC1 &
	./bin/consenso --nodo=ATC2 &
	./bin/consenso --nodo=ATC3 &
	@sleep 2
	@echo "Paso 5/6: Iniciando Clientes MR..."
	./bin/cliente-mr --cliente=MR1 &
	./bin/cliente-mr --cliente=MR2 &
	@sleep 2
	@echo "Paso 6/6: Iniciando Clientes RYW..."
	./bin/cliente-ryw --cliente=RYW1 &
	./bin/cliente-ryw --cliente=RYW2 &
	./bin/cliente-ryw --cliente=RYW3 &
	@echo "✅ Sistema completo iniciado!"
	@echo "💡 Usa 'make stop-all' para detener todos los procesos"

# Detener todos los procesos
stop-all:
	@echo "Deteniendo todos los procesos de AeroDist..."
	@pkill -f "broker|coordinador|datanode|consenso|cliente-mr|cliente-ryw" || true
	@echo "✅ Todos los procesos detenidos"

# Limpiar archivos generados
clean:
	rm -rf bin/
	rm -f broker/proto/*.pb.go
	rm -f clientesMR/proto/*.pb.go
	rm -f clientesRYW/proto/*.pb.go
	rm -f coordinador/proto/*.pb.go
	rm -f datanodes/proto/*.pb.go
	rm -f nodosConsenso/proto/*.pb.go

all: proto build