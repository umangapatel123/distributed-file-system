PROTO_DIR=api/proto
PROTO_FILES=$(wildcard $(PROTO_DIR)/*.proto)

all: proto build

proto:
	protoc -I $(PROTO_DIR) --go_out=$(PROTO_DIR) --go_opt=paths=source_relative --go-grpc_out=$(PROTO_DIR) --go-grpc_opt=paths=source_relative $(PROTO_FILES)

build:
	go build ./...

clean:
	rm -f $(PROTO_DIR)/*.pb.go
