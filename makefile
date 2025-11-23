.PHONY: proto build build-mv1 build-mv2 build-mv3 build-mv4 mv1 mv2 mv3 mv4 start-all stop-all clean logs

# Generar código gRPC
proto:
	mkdir -p broker/proto clientesMR/proto clientesRYW/proto coordinador/proto datanodes/proto nodosConsenso/proto
	protoc --go_out=./broker --go-grpc_out=./broker proto/aeroDist.proto
	protoc --go_out=./clientesMR --go-grpc_out=./clientesMR proto/aeroDist.proto
	protoc --go_out=./clientesRYW --go-grpc_out=./clientesRYW proto/aeroDist.proto
	protoc --go_out=./coordinador --go-grpc_out=./coordinador proto/aeroDist.proto
	protoc --go_out=./datanodes --go-grpc_out=./datanodes proto/aeroDist.proto
	protoc --go_out=./nodosConsenso --go-grpc_out=./nodosConsenso proto/aeroDist.proto

# Build todas las MVs
build: proto
	sudo docker compose -f docker-compose.mv1.yml build
	sudo docker compose -f docker-compose.mv2.yml build
	sudo docker compose -f docker-compose.mv3.yml build
	sudo docker compose -f docker-compose.mv4.yml build

# Build individual por MV
build-mv1: proto
	sudo docker compose -f docker-compose.mv1.yml build

build-mv2: proto
	sudo docker compose -f docker-compose.mv2.yml build

build-mv3: proto
	sudo docker compose -f docker-compose.mv3.yml build

build-mv4: proto
	sudo docker compose -f docker-compose.mv4.yml build

# Ejecución de las 4 máquinas virtuales
mv1: build-mv1
	@echo "Iniciando MV1: Broker + Datanode3 + ClienteRYW1 + ClienteMR1"
	sudo docker compose -f docker-compose.mv1.yml up

mv2: build-mv2
	@echo "Iniciando MV2: Consenso1 + ClienteRYW2 + ClienteMR2"
	sudo docker compose -f docker-compose.mv2.yml up

mv3: build-mv3
	@echo "Iniciando MV3: Datanode1 + Consenso2 + ClienteRYW3"
	sudo docker compose -f docker-compose.mv3.yml up

mv4: build-mv4
	@echo "Iniciando MV4: Coordinador + Datanode2 + Consenso3"
	sudo docker compose -f docker-compose.mv4.yml up

# Ejecución sin build
start-mv1:
	sudo docker compose -f docker-compose.mv1.yml up

start-mv2:
	sudo docker compose -f docker-compose.mv2.yml up

start-mv3:
	sudo docker compose -f docker-compose.mv3.yml up

start-mv4:
	sudo docker compose -f docker-compose.mv4.yml up

# Sistema completo (en terminales separadas)
start-all: 
	@echo "Ejecutar en terminales separadas:"
	@echo "sudo make mv1"
	@echo "sudo make mv2" 
	@echo "sudo make mv3"
	@echo "sudo make mv4"

# Logs
logs-mv1:
	sudo docker compose -f docker-compose.mv1.yml logs -f

logs-mv2:
	sudo docker compose -f docker-compose.mv2.yml logs -f

logs-mv3:
	sudo docker compose -f docker-compose.mv3.yml logs -f

logs-mv4:
	sudo docker compose -f docker-compose.mv4.yml logs -f

# Limpieza
clean:
	@echo "Limpiando contenedores..."
	sudo docker compose -f docker-compose.mv1.yml down -v
	sudo docker compose -f docker-compose.mv2.yml down -v
	sudo docker compose -f docker-compose.mv3.yml down -v
	sudo docker compose -f docker-compose.mv4.yml down -v

stop-all: clean

clean-all: clean
	@echo "Limpiando imágenes..."
	sudo docker image prune -f
