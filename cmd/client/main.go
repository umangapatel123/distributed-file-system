package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	pb "ds-gfs-append/api/proto"
	cli "ds-gfs-append/internal/client"
	"ds-gfs-append/internal/config"
)

func main() {
	command := flag.String("cmd", "create", "operation: create|append|delete")
	remote := flag.String("remote", "", "remote filename in GFS")
	input := flag.String("input", "", "local file to append")
	masterOverride := flag.String("master", "", "master address (override config)")
	cfgPath := flag.String("config", "configs/config.yaml", "config file path")
	flag.Parse()

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "load config:", err)
		os.Exit(1)
	}
	masterAddr := *masterOverride
	if masterAddr == "" {
		masterAddr = cfg.MasterAddress()
	}

	switch *command {
	case "create":
		if *remote == "" {
			fmt.Fprintln(os.Stderr, "--remote is required")
			os.Exit(1)
		}
		if err := cli.CreateFile(masterAddr, *remote); err != nil {
			fmt.Fprintln(os.Stderr, "create file:", err)
			os.Exit(1)
		}
		fmt.Println("created", *remote)
	case "append":
		if *remote == "" || *input == "" {
			fmt.Fprintln(os.Stderr, "--remote and --input are required for append")
			os.Exit(1)
		}
		parts, err := cli.ReadFileChunks(*input, cfg.Chunk.SizeBytes)
		if err != nil {
			fmt.Fprintln(os.Stderr, "read input:", err)
			os.Exit(1)
		}
		meta, err := cli.GetFileChunks(masterAddr, *remote)
		if err != nil {
			fmt.Fprintln(os.Stderr, "describe file:", err)
			os.Exit(1)
		}

		// If file has no chunks, allocate the first chunk
		var chunk *pb.ChunkReplica
		if len(meta.Chunks) == 0 {
			fmt.Println("Allocating first chunk for file...")
			allocResp, err := cli.AllocateChunk(masterAddr, *remote)
			if err != nil {
				fmt.Fprintln(os.Stderr, "allocate chunk:", err)
				os.Exit(1)
			}
			fmt.Printf("Chunk allocated: %s with %d replicas\n", allocResp.Handle, len(allocResp.Replicas))

			// Request lease to assign primary
			if len(allocResp.Replicas) > 0 {
				fmt.Println("Requesting lease from master...")
				leaseResp, err := cli.RequestLease(masterAddr, allocResp.Handle, allocResp.Replicas[0].Address)
				if err != nil {
					fmt.Fprintln(os.Stderr, "request lease:", err)
					os.Exit(1)
				}
				if !leaseResp.Granted {
					fmt.Fprintln(os.Stderr, "lease not granted")
					os.Exit(1)
				}
				// Build chunk replica from lease response
				chunk = &pb.ChunkReplica{
					Handle:              allocResp.Handle,
					Version:             leaseResp.Version,
					Primary:             leaseResp.Primary,
					Secondaries:         leaseResp.Secondaries,
					LeaseExpirationUnix: leaseResp.LeaseExpirationUnix,
				}
				fmt.Printf("Lease granted: primary=%s, version=%d\n", chunk.Primary.Address, chunk.Version)
			} else {
				fmt.Fprintln(os.Stderr, "no replicas available for chunk")
				os.Exit(1)
			}
		} else {
			chunk = meta.Chunks[0]
		}

		primaryAddr, secondaries := extractReplicas(chunk)
		if primaryAddr == "" {
			fmt.Fprintln(os.Stderr, "no primary replica available")
			os.Exit(1)
		}
		replicas := append([]string{primaryAddr}, secondaries...)

		for idx, part := range parts {
			stamp := time.Now().UnixNano()
			dataID := fmt.Sprintf("%s-data-%d-%d", chunk.Handle, stamp, idx)
			appendID := fmt.Sprintf("%s-append-%d-%d", chunk.Handle, stamp, idx)
			dataID, err = cli.PushToReplicas(replicas, dataID, part)
			if err != nil {
				fmt.Fprintln(os.Stderr, "push data:", err)
				os.Exit(1)
			}
			if _, err := cli.RetryAppend(primaryAddr, secondaries, chunk.Handle, dataID, appendID, chunk.Version, 3); err != nil {
				fmt.Fprintln(os.Stderr, "append failed:", err)
				os.Exit(1)
			}
			fmt.Println("appended chunk", idx, "bytes", len(part))
		}
	case "delete":
		if *remote == "" {
			fmt.Fprintln(os.Stderr, "--remote is required")
			os.Exit(1)
		}
		if err := cli.DeleteFile(masterAddr, *remote); err != nil {
			fmt.Fprintln(os.Stderr, "delete file:", err)
			os.Exit(1)
		}
		fmt.Println("deleted", *remote)
	default:
		fmt.Println("unknown cmd", *command)
	}
}

func extractReplicas(chunk *pb.ChunkReplica) (string, []string) {
	primary := ""
	if chunk.Primary != nil {
		primary = chunk.Primary.Address
	}
	secondaries := make([]string, 0, len(chunk.Secondaries))
	for _, loc := range chunk.Secondaries {
		secondaries = append(secondaries, loc.Address)
	}
	if primary == "" && len(secondaries) > 0 {
		primary = secondaries[0]
		secondaries = secondaries[1:]
	}
	return primary, secondaries
}
