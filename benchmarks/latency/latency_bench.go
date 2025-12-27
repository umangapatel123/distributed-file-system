package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"sync"
	"time"

	pb "ds-gfs-append/api/proto"
	"ds-gfs-append/internal/client"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type LatencyStats struct {
	Samples       []time.Duration
	P50           time.Duration
	P95           time.Duration
	P99           time.Duration
	P999          time.Duration
	Min           time.Duration
	Max           time.Duration
	Mean          time.Duration
	TotalOps      int
	SuccessfulOps int
	FailedOps     int
}

func dialMaster(addr string) (*grpc.ClientConn, error) {
	return grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
}

func createFile(masterAddr, filename string) error {
	conn, err := dialMaster(masterAddr)
	if err != nil {
		return err
	}
	defer conn.Close()

	mc := pb.NewMasterClient(conn)
	_, err = mc.CreateFile(context.Background(), &pb.CreateFileRequest{Filename: filename})
	return err
}

func measureLatency(masterAddr string, filename string, numOps int, numClients int, payloadSize int) LatencyStats {
	payload := make([]byte, payloadSize)
	for i := range payload {
		payload[i] = byte('A' + (i % 26))
	}

	var mu sync.Mutex
	latencies := make([]time.Duration, 0, numOps)
	successCount := 0
	failCount := 0

	opsPerClient := numOps / numClients
	if opsPerClient == 0 {
		opsPerClient = 1
	}

	var wg sync.WaitGroup

	for clientID := 0; clientID < numClients; clientID++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			conn, err := dialMaster(masterAddr)
			if err != nil {
				mu.Lock()
				failCount += opsPerClient
				mu.Unlock()
				return
			}
			defer conn.Close()
			mc := pb.NewMasterClient(conn)

			for i := 0; i < opsPerClient; i++ {
				start := time.Now()

				// Get or allocate chunk
				var chunkHandle string
				var chunkVersion int64
				var primary string
				var secondaries []string

				infoResp, err := mc.GetFileChunksInfo(context.Background(), &pb.GetFileChunksInfoRequest{
					Filename: filename,
				})
				if err != nil || len(infoResp.Chunks) == 0 {
					allocResp, err := mc.AllocateChunk(context.Background(), &pb.AllocateChunkRequest{
						Filename: filename,
					})
					if err != nil {
						mu.Lock()
						failCount++
						mu.Unlock()
						continue
					}
					chunkHandle = allocResp.Handle
					if len(allocResp.Replicas) > 0 {
						primary = allocResp.Replicas[0].Address
						for j := 1; j < len(allocResp.Replicas); j++ {
							secondaries = append(secondaries, allocResp.Replicas[j].Address)
						}
					}
					chunkVersion = 1
				} else {
					chunk := infoResp.Chunks[0]
					chunkHandle = chunk.Handle
					chunkVersion = chunk.Version
					if chunk.Primary != nil {
						primary = chunk.Primary.Address
					}
					for _, sec := range chunk.Secondaries {
						secondaries = append(secondaries, sec.Address)
					}
				}

				if primary == "" {
					mu.Lock()
					failCount++
					mu.Unlock()
					continue
				}

				// Generate unique IDs
				dataID := fmt.Sprintf("bench-latency-data-%d-%d-%d", id, i, time.Now().UnixNano())
				appendID := fmt.Sprintf("bench-latency-append-%d-%d-%d", id, i, time.Now().UnixNano())

				// Push data
				_, err = client.PushToReplicas(append([]string{primary}, secondaries...), dataID, payload)
				if err != nil {
					mu.Lock()
					failCount++
					mu.Unlock()
					continue
				}

				// Append record
				appendResp, err := client.AppendRecord(primary, chunkHandle, dataID, appendID, secondaries, chunkVersion)
				if err != nil || !appendResp.Success {
					mu.Lock()
					failCount++
					mu.Unlock()
					continue
				}

				latency := time.Since(start)

				mu.Lock()
				latencies = append(latencies, latency)
				successCount++
				mu.Unlock()
			}
		}(clientID)
	}

	wg.Wait()

	stats := LatencyStats{
		Samples:       latencies,
		TotalOps:      numOps,
		SuccessfulOps: successCount,
		FailedOps:     failCount,
	}

	if len(latencies) == 0 {
		return stats
	}

	// Sort for percentile calculation
	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})

	stats.Min = latencies[0]
	stats.Max = latencies[len(latencies)-1]

	// Calculate percentiles
	stats.P50 = latencies[len(latencies)*50/100]
	stats.P95 = latencies[len(latencies)*95/100]
	stats.P99 = latencies[len(latencies)*99/100]
	if len(latencies) > 1000 {
		stats.P999 = latencies[len(latencies)*999/1000]
	} else {
		stats.P999 = stats.Max
	}

	// Calculate mean
	var sum time.Duration
	for _, lat := range latencies {
		sum += lat
	}
	stats.Mean = sum / time.Duration(len(latencies))

	return stats
}

func main() {
	masterAddr := flag.String("master", "127.0.0.1:50051", "master server address")
	numOps := flag.Int("ops", 1000, "total number of operations")
	numClients := flag.Int("clients", 10, "number of concurrent clients")
	payloadSize := flag.Int("payload", 1024, "payload size in bytes")
	outputFile := flag.String("output", "", "output file for results (optional)")
	flag.Parse()

	fmt.Printf("Latency Benchmark\n")
	fmt.Printf("=================\n")
	fmt.Printf("Master: %s\n", *masterAddr)
	fmt.Printf("Total Operations: %d\n", *numOps)
	fmt.Printf("Concurrent Clients: %d\n", *numClients)
	fmt.Printf("Payload Size: %d bytes\n\n", *payloadSize)

	// Create test file
	filename := fmt.Sprintf("/bench-latency-%d", rand.Int63())
	fmt.Printf("Creating file: %s\n", filename)
	if err := createFile(*masterAddr, filename); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create file: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("File created successfully\n")

	// Wait a bit for system to stabilize
	time.Sleep(2 * time.Second)

	// Run benchmark
	fmt.Println("Measuring latency...")
	stats := measureLatency(*masterAddr, filename, *numOps, *numClients, *payloadSize)

	// Print results
	fmt.Printf("\nResults:\n")
	fmt.Printf("========\n")
	fmt.Printf("Total Operations:     %d\n", stats.TotalOps)
	fmt.Printf("Successful:           %d\n", stats.SuccessfulOps)
	fmt.Printf("Failed:               %d\n", stats.FailedOps)
	fmt.Printf("Min Latency:          %v\n", stats.Min)
	fmt.Printf("P50 Latency:          %v\n", stats.P50)
	fmt.Printf("P95 Latency:          %v\n", stats.P95)
	fmt.Printf("P99 Latency:          %v\n", stats.P99)
	fmt.Printf("P99.9 Latency:        %v\n", stats.P999)
	fmt.Printf("Max Latency:          %v\n", stats.Max)
	fmt.Printf("Mean Latency:         %v\n", stats.Mean)

	// Write to file if specified
	if *outputFile != "" {
		f, err := os.OpenFile(*outputFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to open output file: %v\n", err)
			return
		}
		defer f.Close()

		timestamp := time.Now().Format("2006-01-02 15:04:05")
		fmt.Fprintf(f, "%s,latency,%d,%d,%d,%v,%v,%v,%v,%v,%v,%v\n",
			timestamp, *numClients, *payloadSize, stats.SuccessfulOps,
			stats.Min, stats.P50, stats.P95, stats.P99, stats.P999, stats.Max, stats.Mean)
	}
}
