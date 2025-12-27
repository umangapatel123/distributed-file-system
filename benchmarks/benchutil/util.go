package benchutil

import (
	"context"
	"fmt"

	pb "ds-gfs-append/api/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func DialMaster(addr string) (*grpc.ClientConn, error) {
	return grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
}

func CreateFile(masterAddr, filename string) error {
	conn, err := DialMaster(masterAddr)
	if err != nil {
		return err
	}
	defer conn.Close()

	mc := pb.NewMasterClient(conn)
	_, err = mc.CreateFile(context.Background(), &pb.CreateFileRequest{Filename: filename})
	return err
}

type ChunkInfo struct {
	Handle      string
	Version     int64
	Primary     string
	Secondaries []string
}

func GetOrAllocateChunk(mc pb.MasterClient, filename string) (*ChunkInfo, error) {
	// First try to get existing chunks
	infoResp, err := mc.GetFileChunksInfo(context.Background(), &pb.GetFileChunksInfoRequest{
		Filename: filename,
	})

	if err != nil || len(infoResp.Chunks) == 0 {
		// Allocate new chunk
		allocResp, err := mc.AllocateChunk(context.Background(), &pb.AllocateChunkRequest{
			Filename: filename,
		})
		if err != nil {
			return nil, err
		}

		info := &ChunkInfo{
			Handle:      allocResp.Handle,
			Version:     1,
			Secondaries: make([]string, 0),
		}

		if len(allocResp.Replicas) > 0 {
			info.Primary = allocResp.Replicas[0].Address
			for i := 1; i < len(allocResp.Replicas); i++ {
				info.Secondaries = append(info.Secondaries, allocResp.Replicas[i].Address)
			}
		}

		if info.Primary == "" {
			return nil, fmt.Errorf("no primary replica available")
		}

		return info, nil
	}

	// Use existing chunk
	chunk := infoResp.Chunks[0]
	info := &ChunkInfo{
		Handle:      chunk.Handle,
		Version:     chunk.Version,
		Secondaries: make([]string, 0),
	}

	if chunk.Primary != nil {
		info.Primary = chunk.Primary.Address
	}

	for _, sec := range chunk.Secondaries {
		info.Secondaries = append(info.Secondaries, sec.Address)
	}

	if info.Primary == "" {
		return nil, fmt.Errorf("no primary replica available")
	}

	return info, nil
}
