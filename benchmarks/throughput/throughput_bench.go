package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	pb "ds-gfs-append/api/proto"
	"ds-gfs-append/internal/client"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type BenchmarkResult struct {
	TotalOps       int64
	Duration       time.Duration
	ThroughputOpsS float64
	ThroughputMBs  float64
	SuccessfulOps  int64
	FailedOps      int64
	AvgLatencyMs   float64
	MinLatencyMs   float64
	MaxLatencyMs   float64
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

func benchmarkAppend(masterAddr string, filename string, numClients int, payloadSize int, duration time.Duration) BenchmarkResult {
	var totalOps int64
	var successfulOps int64
	var failedOps int64
	var totalLatencyMs int64
	var minLatency int64 = 1<<63 - 1
	var maxLatency int64

	payload := make([]byte, payloadSize)
	for i := range payload {
		payload[i] = byte('A' + (i % 26))
	}

	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	var wg sync.WaitGroup
	startTime := time.Now()

	for clientID := 0; clientID < numClients; clientID++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			conn, err := dialMaster(masterAddr)
			if err != nil {
				atomic.AddInt64(&failedOps, 1)
				return
			}
			defer conn.Close()
			mc := pb.NewMasterClient(conn)

			localOps := 0
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				opStart := time.Now()

				// Get or allocate chunk
				var chunkHandle string
				var chunkVersion int64
				var primary string
				var secondaries []string

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
						atomic.AddInt64(&failedOps, 1)
						continue
					}
					chunkHandle = allocResp.Handle
					if len(allocResp.Replicas) > 0 {
						primary = allocResp.Replicas[0].Address
						for i := 1; i < len(allocResp.Replicas); i++ {
							secondaries = append(secondaries, allocResp.Replicas[i].Address)
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
					atomic.AddInt64(&failedOps, 1)
					continue
				}

				// Generate unique IDs
				dataID := fmt.Sprintf("bench-data-%d-%d-%d", id, localOps, time.Now().UnixNano())
				appendID := fmt.Sprintf("bench-append-%d-%d-%d", id, localOps, time.Now().UnixNano())

				// Push data to all replicas
				_, err = client.PushToReplicas(append([]string{primary}, secondaries...), dataID, payload)
				if err != nil {
					atomic.AddInt64(&failedOps, 1)
					continue
				}

				// Append record
				appendResp, err := client.AppendRecord(primary, chunkHandle, dataID, appendID, secondaries, chunkVersion)
				if err != nil || !appendResp.Success {
					atomic.AddInt64(&failedOps, 1)
					continue
				}

				latency := time.Since(opStart).Milliseconds()
				atomic.AddInt64(&totalLatencyMs, latency)
				atomic.AddInt64(&totalOps, 1)
				atomic.AddInt64(&successfulOps, 1)

				// Update min/max latency
				for {
					currentMin := atomic.LoadInt64(&minLatency)
					if latency >= currentMin || atomic.CompareAndSwapInt64(&minLatency, currentMin, latency) {
						break
					}
				}
				for {
					currentMax := atomic.LoadInt64(&maxLatency)
					if latency <= currentMax || atomic.CompareAndSwapInt64(&maxLatency, currentMax, latency) {
						break
					}
				}

				localOps++
			}
		}(clientID)
	}

	wg.Wait()
	elapsed := time.Since(startTime)

	result := BenchmarkResult{
		TotalOps:      totalOps,
		Duration:      elapsed,
		SuccessfulOps: successfulOps,
		FailedOps:     failedOps,
	}

	if totalOps > 0 {
		result.ThroughputOpsS = float64(totalOps) / elapsed.Seconds()
		result.ThroughputMBs = float64(totalOps*int64(payloadSize)) / elapsed.Seconds() / (1024 * 1024)
		result.AvgLatencyMs = float64(totalLatencyMs) / float64(totalOps)
		result.MinLatencyMs = float64(minLatency)
		result.MaxLatencyMs = float64(maxLatency)
	}

	return result
}

func main() {
	masterAddr := flag.String("master", "127.0.0.1:50051", "master server address")
	numClients := flag.Int("clients", 10, "number of concurrent clients")
	payloadSize := flag.Int("payload", 1024, "payload size in bytes")
	durationSec := flag.Int("duration", 30, "benchmark duration in seconds")
	outputFile := flag.String("output", "", "output file for results (optional)")
	flag.Parse()

	fmt.Printf("Throughput Benchmark\n")
	fmt.Printf("====================\n")
	fmt.Printf("Master: %s\n", *masterAddr)
	fmt.Printf("Clients: %d\n", *numClients)
	fmt.Printf("Payload Size: %d bytes\n", *payloadSize)
	fmt.Printf("Duration: %d seconds\n\n", *durationSec)

	// Create test file
	filename := fmt.Sprintf("/bench-throughput-%d", rand.Int63())
	fmt.Printf("Creating file: %s\n", filename)
	if err := createFile(*masterAddr, filename); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create file: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("File created successfully\n")

	// Wait a bit for system to stabilize
	time.Sleep(2 * time.Second)

	// Run benchmark
	fmt.Println("Running benchmark...")
	result := benchmarkAppend(*masterAddr, filename, *numClients, *payloadSize, time.Duration(*durationSec)*time.Second)

	// Print results
	fmt.Printf("\nResults:\n")
	fmt.Printf("========\n")
	fmt.Printf("Total Operations:     %d\n", result.TotalOps)
	fmt.Printf("Successful:           %d\n", result.SuccessfulOps)
	fmt.Printf("Failed:               %d\n", result.FailedOps)
	fmt.Printf("Duration:             %.2f seconds\n", result.Duration.Seconds())
	fmt.Printf("Throughput:           %.2f ops/sec\n", result.ThroughputOpsS)
	fmt.Printf("Data Throughput:      %.2f MB/s\n", result.ThroughputMBs)
	fmt.Printf("Avg Latency:          %.2f ms\n", result.AvgLatencyMs)
	fmt.Printf("Min Latency:          %.2f ms\n", result.MinLatencyMs)
	fmt.Printf("Max Latency:          %.2f ms\n", result.MaxLatencyMs)

	// Write to file if specified
	if *outputFile != "" {
		f, err := os.OpenFile(*outputFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to open output file: %v\n", err)
			return
		}
		defer f.Close()

		timestamp := time.Now().Format("2006-01-02 15:04:05")
		fmt.Fprintf(f, "%s,throughput,%d,%d,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f\n",
			timestamp, *numClients, *payloadSize,
			result.ThroughputOpsS, result.ThroughputMBs,
			result.AvgLatencyMs, result.MinLatencyMs, result.MaxLatencyMs,
			result.Duration.Seconds())
	}
}
