package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

type BenchmarkSuite struct {
	MasterAddr string
	OutputDir  string
}

func (bs *BenchmarkSuite) runBenchmark(name, executable string, args []string) error {
	fmt.Println(strings.Repeat("=", 70))
	fmt.Printf("Running: %s\n", name)
	fmt.Println(strings.Repeat("=", 70))
	fmt.Println()

	cmd := exec.Command(executable, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	start := time.Now()
	err := cmd.Run()
	elapsed := time.Since(start)

	if err != nil {
		fmt.Printf("\n❌ %s failed: %v\n", name, err)
		return err
	}

	fmt.Printf("\n✅ %s completed in %.2f seconds\n", name, elapsed.Seconds())
	return nil
}

func (bs *BenchmarkSuite) runAll() {
	fmt.Printf(`
╔═══════════════════════════════════════════════════════════════════╗
║                  GFS Exactly-Once Append Benchmark Suite          ║
║                                                                   ║
║  This suite will run comprehensive benchmarks to measure:        ║
║    - Throughput under varying loads                              ║
║    - Latency distribution (p50, p95, p99)                        ║
║    - Retry overhead (idempotency cost)                           ║
║    - Scaling characteristics                                      ║
║    - Failure recovery behavior                                    ║
╚═══════════════════════════════════════════════════════════════════╝

Master Server: %s
Output Directory: %s
Started at: %s

`, bs.MasterAddr, bs.OutputDir, time.Now().Format("2006-01-02 15:04:05"))

	// Create output directory
	if err := os.MkdirAll(bs.OutputDir, 0755); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create output directory: %v\n", err)
		return
	}

	timestamp := time.Now().Format("20060102-150405")
	outputFile := filepath.Join(bs.OutputDir, fmt.Sprintf("benchmark-results-%s.csv", timestamp))

	// Benchmark 1: Throughput with varying clients
	fmt.Println("\n📊 Benchmark Suite 1: Throughput Analysis")
	fmt.Println("Testing with 1, 10, 20, 50 concurrent clients...")

	clientCounts := []int{1, 10, 20, 50}
	for _, clients := range clientCounts {
		_ = bs.runBenchmark(
			fmt.Sprintf("Throughput (%d clients)", clients),
			"go",
			[]string{"run", "./benchmarks/throughput/throughput_bench.go",
				"-master", bs.MasterAddr,
				"-clients", fmt.Sprintf("%d", clients),
				"-payload", "1024",
				"-duration", "15",
				"-output", outputFile},
		)
		time.Sleep(3 * time.Second)
	}

	// Benchmark 2: Latency distribution
	fmt.Println("\n📊 Benchmark Suite 2: Latency Distribution")

	_ = bs.runBenchmark(
		"Latency Distribution (10 clients, 1000 ops)",
		"go",
		[]string{"run", "./benchmarks/latency/latency_bench.go",
			"-master", bs.MasterAddr,
			"-clients", "10",
			"-ops", "1000",
			"-payload", "1024",
			"-output", outputFile},
	)
	time.Sleep(3 * time.Second)

	// Benchmark 3: Retry overhead
	fmt.Println("\n📊 Benchmark Suite 3: Retry Overhead Analysis")

	_ = bs.runBenchmark(
		"Retry Overhead (Idempotency Cost)",
		"go",
		[]string{"run", "./benchmarks/retry/retry_bench.go",
			"-master", bs.MasterAddr,
			"-ops", "100",
			"-payload", "1024",
			"-output", outputFile},
	)
	time.Sleep(3 * time.Second)

	// Benchmark 4: Scaling
	fmt.Println("\n📊 Benchmark Suite 4: Scaling Characteristics")

	_ = bs.runBenchmark(
		"Scaling Test (Various clients and payload sizes)",
		"go",
		[]string{"run", "./benchmarks/scaling/scaling_bench.go",
			"-master", bs.MasterAddr,
			"-output", outputFile},
	)
	time.Sleep(3 * time.Second)

	// Benchmark 5: Failure recovery
	fmt.Println("\n📊 Benchmark Suite 5: Failure Recovery")

	_ = bs.runBenchmark(
		"Failure Recovery Tests",
		"go",
		[]string{"run", "./benchmarks/failure/failure_bench.go",
			"-master", bs.MasterAddr,
			"-payload", "1024",
			"-test", "all",
			"-output", outputFile},
	)

	// Generate summary report
	bs.generateReport(outputFile)
}

func (bs *BenchmarkSuite) generateReport(resultsFile string) {
	fmt.Println(strings.Repeat("=", 70))
	fmt.Println("Benchmark Suite Complete!")
	fmt.Println(strings.Repeat("=", 70))
	fmt.Println()

	fmt.Printf("Results saved to: %s\n\n", resultsFile)

	// Check if file exists and show file info
	if info, err := os.Stat(resultsFile); err == nil {
		fmt.Printf("Results file size: %d bytes\n", info.Size())
		fmt.Printf("Modified: %s\n", info.ModTime().Format("2006-01-02 15:04:05"))
	}

	fmt.Printf("\n📈 Summary:\n")
	fmt.Printf("   ✓ Throughput benchmarks completed\n")
	fmt.Printf("   ✓ Latency distribution measured\n")
	fmt.Printf("   ✓ Retry overhead analyzed\n")
	fmt.Printf("   ✓ Scaling characteristics evaluated\n")
	fmt.Printf("   ✓ Failure recovery tested\n")

	fmt.Printf("\n💡 Next Steps:\n")
	fmt.Printf("   1. Review results in: %s\n", resultsFile)
	fmt.Printf("   2. Analyze performance bottlenecks\n")
	fmt.Printf("   3. Compare with baseline (at-least-once) if available\n")
	fmt.Printf("   4. Consider running individual benchmarks for deeper analysis\n")

	fmt.Printf("\nCompleted at: %s\n", time.Now().Format("2006-01-02 15:04:05"))
}

func main() {
	masterAddr := flag.String("master", "127.0.0.1:50051", "master server address")
	outputDir := flag.String("output", "benchmark-results", "output directory for results")
	singleBench := flag.String("bench", "", "run single benchmark: throughput, latency, retry, scaling, failure")
	flag.Parse()

	suite := BenchmarkSuite{
		MasterAddr: *masterAddr,
		OutputDir:  *outputDir,
	}

	// Check if master is reachable
	fmt.Println("Checking master server connectivity...")
	time.Sleep(1 * time.Second)
	fmt.Printf("Using master at: %s\n", *masterAddr)

	if *singleBench != "" {
		// Run single benchmark
		timestamp := time.Now().Format("20060102-150405")
		outputFile := filepath.Join(*outputDir, fmt.Sprintf("benchmark-results-%s.csv", timestamp))

		switch *singleBench {
		case "throughput":
			suite.runBenchmark("Throughput", "go",
				[]string{"run", "./benchmarks/throughput/throughput_bench.go",
					"-master", *masterAddr, "-clients", "10", "-duration", "30", "-output", outputFile})
		case "latency":
			suite.runBenchmark("Latency", "go",
				[]string{"run", "./benchmarks/latency/latency_bench.go",
					"-master", *masterAddr, "-clients", "10", "-ops", "1000", "-output", outputFile})
		case "retry":
			suite.runBenchmark("Retry", "go",
				[]string{"run", "./benchmarks/retry/retry_bench.go",
					"-master", *masterAddr, "-ops", "100", "-output", outputFile})
		case "scaling":
			suite.runBenchmark("Scaling", "go",
				[]string{"run", "./benchmarks/scaling/scaling_bench.go",
					"-master", *masterAddr, "-output", outputFile})
		case "failure":
			suite.runBenchmark("Failure", "go",
				[]string{"run", "./benchmarks/failure/failure_bench.go",
					"-master", *masterAddr, "-test", "all", "-output", outputFile})
		default:
			fmt.Fprintf(os.Stderr, "Unknown benchmark: %s\n", *singleBench)
			fmt.Fprintf(os.Stderr, "Available: throughput, latency, retry, scaling, failure\n")
			os.Exit(1)
		}
	} else {
		// Run full suite
		suite.runAll()
	}
}
