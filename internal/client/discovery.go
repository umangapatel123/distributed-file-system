package client

import (
	"context"

	pb "ds-gfs-append/api/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func DialMaster(addr string) (*grpc.ClientConn, error) {
	return grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
}

func GetFileChunks(masterAddr, filename string) (*pb.GetFileChunksInfoResponse, error) {
	conn, err := DialMaster(masterAddr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	c := pb.NewMasterClient(conn)
	return c.GetFileChunksInfo(context.Background(), &pb.GetFileChunksInfoRequest{Filename: filename})
}

func CreateFile(masterAddr, filename string) error {
	conn, err := DialMaster(masterAddr)
	if err != nil {
		return err
	}
	defer conn.Close()
	c := pb.NewMasterClient(conn)
	_, err = c.CreateFile(context.Background(), &pb.CreateFileRequest{Filename: filename})
	return err
}

func AllocateChunk(masterAddr, filename string) (*pb.AllocateChunkResponse, error) {
	conn, err := DialMaster(masterAddr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	c := pb.NewMasterClient(conn)
	return c.AllocateChunk(context.Background(), &pb.AllocateChunkRequest{Filename: filename})
}

func RequestLease(masterAddr, chunkHandle, requester string) (*pb.RequestLeaseResponse, error) {
	conn, err := DialMaster(masterAddr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	c := pb.NewMasterClient(conn)
	return c.RequestLease(context.Background(), &pb.RequestLeaseRequest{
		ChunkHandle: chunkHandle,
		Requester:   requester,
	})
}

func DeleteFile(masterAddr, filename string) error {
	conn, err := DialMaster(masterAddr)
	if err != nil {
		return err
	}
	defer conn.Close()
	c := pb.NewMasterClient(conn)
	_, err = c.DeleteFile(context.Background(), &pb.DeleteFileRequest{Filename: filename})
	return err
}

func RenameFile(masterAddr, oldName, newName string) error {
	conn, err := DialMaster(masterAddr)
	if err != nil {
		return err
	}
	defer conn.Close()
	c := pb.NewMasterClient(conn)
	_, err = c.RenameFile(context.Background(), &pb.RenameFileRequest{OldName: oldName, NewName: newName})
	return err
}

func ListFiles(masterAddr string) ([]string, error) {
	conn, err := DialMaster(masterAddr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	c := pb.NewMasterClient(conn)
	resp, err := c.ListFiles(context.Background(), &pb.ListFilesRequest{})
	if err != nil {
		return nil, err
	}
	return resp.GetFilenames(), nil
}
