package client

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	pb "ds-gfs-append/api/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func dialChunk(addr string) (*grpc.ClientConn, error) {
	return grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
}

// PushDataTo streams the payload to a chunkserver buffer and returns the resulting dataID.
func PushDataTo(csAddr, dataID string, data []byte) (string, error) {
	conn, err := dialChunk(csAddr)
	if err != nil {
		return "", err
	}
	defer conn.Close()
	c := pb.NewChunkServerClient(conn)
	stream, err := c.PushData(context.Background())
	if err != nil {
		return "", err
	}
	if err := stream.Send(&pb.PushDataRequest{DataId: dataID, Data: data}); err != nil {
		return "", err
	}
	resp, err := stream.CloseAndRecv()
	if err != nil {
		return "", err
	}
	if resp.DataId != "" {
		dataID = resp.DataId
	}
	return dataID, nil
}

func PushToReplicas(replicas []string, dataID string, data []byte) (string, error) {
	var err error
	for _, addr := range replicas {
		dataID, err = PushDataTo(addr, dataID, data)
		if err != nil {
			return "", err
		}
	}
	return dataID, nil
}

// AppendRecord asks the primary to commit the buffered data for a chunk version.
func AppendRecord(primaryAddr string, chunkHandle string, dataID string, appendID string, secondaries []string, version int64) (*pb.AppendRecordResponse, error) {
	conn, err := dialChunk(primaryAddr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	c := pb.NewChunkServerClient(conn)
	secs := make([]*pb.ChunkLocation, 0, len(secondaries))
	for _, s := range secondaries {
		secs = append(secs, &pb.ChunkLocation{Address: s})
	}
	return c.AppendRecord(context.Background(), &pb.AppendRecordRequest{
		ChunkHandle:   chunkHandle,
		DataId:        dataID,
		Secondaries:   secs,
		ChunkVersion:  version,
		IdempotencyId: appendID,
	})
}

// Helper to read file and split into chunk-sized blocks (simple chunker for demo purposes).
func ReadFileChunks(path string, maxChunkSize int64) ([][]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	if maxChunkSize <= 0 {
		maxChunkSize = 64 << 20
	}
	var out [][]byte
	buf := make([]byte, maxChunkSize)
	for {
		n, err := io.ReadFull(f, buf)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			if n > 0 {
				b := make([]byte, n)
				copy(b, buf[:n])
				out = append(out, b)
			}
			break
		}
		if err != nil {
			return nil, err
		}
		b := make([]byte, n)
		copy(b, buf[:n])
		out = append(out, b)
	}
	return out, nil
}

// RetryAppend retries the append operation with exactly-once semantics using the provided idempotency ID.
func RetryAppend(primary string, secondaries []string, chunkHandle string, dataID string, appendID string, version int64, maxRetries int) (*pb.AppendRecordResponse, error) {
	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		resp, err := AppendRecord(primary, chunkHandle, dataID, appendID, secondaries, version)
		if err == nil && resp != nil && resp.Success {
			return resp, nil
		}
		if err != nil {
			lastErr = err
		}
		time.Sleep(500 * time.Millisecond)
	}
	return nil, fmt.Errorf("append failed after retries: %v", lastErr)
}

// WriteChunk asks the primary to write buffered data at the provided offset.
func WriteChunk(primaryAddr string, chunkHandle string, dataID string, secondaries []string, version int64, offset int64) (*pb.WriteChunkResponse, error) {
	conn, err := dialChunk(primaryAddr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	c := pb.NewChunkServerClient(conn)
	secs := make([]*pb.ChunkLocation, 0, len(secondaries))
	for _, s := range secondaries {
		secs = append(secs, &pb.ChunkLocation{Address: s})
	}
	return c.WriteChunk(context.Background(), &pb.WriteChunkRequest{
		ChunkHandle:  chunkHandle,
		DataId:       dataID,
		Secondaries:  secs,
		ChunkVersion: version,
		Offset:       offset,
	})
}

// ReadChunk fetches data from a replica at the specified offset and length.
func ReadChunk(addr, chunkHandle string, offset, length int64) ([]byte, error) {
	conn, err := dialChunk(addr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	c := pb.NewChunkServerClient(conn)
	resp, err := c.ReadChunk(context.Background(), &pb.ReadChunkRequest{
		ChunkHandle: chunkHandle,
		Offset:      offset,
		Length:      length,
	})
	if err != nil {
		return nil, err
	}
	return resp.GetData(), nil
}
