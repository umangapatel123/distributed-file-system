# GFS-Once-Append: Distributed File System with Exactly-Once Append Semantics

A distributed file system implementation inspired by the Google File System (GFS) architecture, built in Go with enhanced support for exactly-once append semantics using modified two-phase commit protocol.

## Features

- **64MB Chunk-Based Storage**: Large file support with efficient chunk management
- **3-Way Replication**: Automatic data replication for fault tolerance
- **Exactly-Once Append Semantics**: Idempotent append operations using modified 2PC
- **Primary-Secondary Replication**: Lease-based coordination for write operations
- **Write-Ahead Logging (WAL)**: Master crash recovery with operation log
- **Heartbeat-Based Failure Detection**: Automatic monitoring and failure recovery
- **Garbage Collection**: Automatic cleanup of orphaned chunks
- **Persistent State Management**: Durable chunk server state across restarts
- **Comprehensive Benchmarking**: Built-in latency and throughput analysis tools

## Prerequisites

- **Go 1.23.0 or higher**
- **Protocol Buffers compiler (protoc)**
- **Make** (for build automation)

## Project Structure

```
gfs-once-append/
├── api/
│   └── proto/                   # Protocol Buffer definitions
│       ├── common.proto         # Shared message types
│       ├── master.proto         # Master server RPC definitions
│       └── chunkserver.proto    # ChunkServer RPC definitions
│
├── cmd/
│   ├── master/                  # Master server executable
│   ├── chunkserver/             # ChunkServer executable
│   ├── client/                  # CLI client for basic operations
│   └── interactive/             # Interactive REPL client
│
├── internal/
│   ├── master/                  # Master server implementation
│   │   ├── master.go           # Core master logic
│   │   ├── metadata.go         # Metadata management
│   │   ├── lease.go            # Lease coordination
│   │   ├── operation_log.go    # Write-ahead logging
│   │   └── replication.go      # Replication management
│   │
│   ├── chunkserver/            # ChunkServer implementation
│   │   ├── chunkserver.go      # Core chunkserver logic
│   │   ├── handler.go          # RPC handlers
│   │   ├── append_state.go     # Exactly-once append tracking
│   │   ├── data_store.go       # Chunk storage management
│   │   ├── state_store.go      # Persistent state
│   │   └── recovery.go         # Crash recovery
│   │
│   ├── client/                 # Client library
│   │   ├── client.go           # Client operations
│   │   └── discovery.go        # Master discovery
│   │
│   ├── config/                 # Configuration management
│   └── logger/                 # Logging utilities
│
├── benchmarks/                 # Performance benchmarking suite
│   ├── latency/               # Latency benchmarks
│   ├── throughput/            # Throughput benchmarks
│   ├── benchutil/             # Benchmark utilities
│   └── run_benchmarks.go      # Benchmark runner
│
├── configs/
│   └── config.yaml            # System configuration
│
├── Makefile                   # Build automation
├── go.mod                     # Go module definition
├── REPORT.md                  # Comprehensive technical report
└── README.md                  # This file
```

## Installation & Build

### 1. Clone the Repository

```bash
git clone https://github.com/umangapatel123/distributed-file-system.git
cd distributed-file-system
```

### 2. Install Dependencies

```bash
go mod download
```

### 3. Generate Protocol Buffer Code

```bash
make proto
```

### 4. Build All Components

```bash
make build
```

Or build everything at once:

```bash
make all
```

## Running the System

### Quick Start - Single Machine Setup

#### Step 1: Start the Master Server

```bash
go run cmd/master/main.go -addr :50051 -log operation.log -loglevel INFO
```

**Options:**
- `-addr`: Master server listen address (default: `127.0.0.1:50051`)
- `-log`: Operation log file path (default: `operation.log`)
- `-meta`: Metadata snapshot file path (default: `metadata_snapshot.json`)
- `-config`: Configuration file path (default: `configs/config.yaml`)
- `-loglevel`: Log verbosity level (`NONE`, `ERROR`, `WARN`, `INFO`, `DEBUG`)

#### Step 2: Start ChunkServers

Open separate terminals for each chunk server:

**ChunkServer 1:**
```bash
go run cmd/chunkserver/main.go -addr :6000 -data ./data/cs1 -master 127.0.0.1:50051 -loglevel INFO
```

**ChunkServer 2:**
```bash
go run cmd/chunkserver/main.go -addr :6001 -data ./data/cs2 -master 127.0.0.1:50051 -loglevel INFO
```

**ChunkServer 3:**
```bash
go run cmd/chunkserver/main.go -addr :6002 -data ./data/cs3 -master 127.0.0.1:50051 -loglevel INFO
```

**ChunkServer 4 (optional):**
```bash
go run cmd/chunkserver/main.go -addr :6003 -data ./data/cs4 -master 127.0.0.1:50051 -loglevel INFO
```

**Options:**
- `-addr`: ChunkServer listen address
- `-data`: Data directory for storing chunks
- `-master`: Master server address
- `-config`: Configuration file path
- `-loglevel`: Log verbosity level

#### Step 3: Use the Client

**Create a file:**
```bash
go run cmd/client/main.go -cmd create -remote /myfile.txt -master 127.0.0.1:50051
```

**Append data:**
```bash
# Create a local file to append
echo "Hello, GFS!" > local_data.txt

# Append to remote file
go run cmd/client/main.go -cmd append -remote /myfile.txt -input local_data.txt -master 127.0.0.1:50051
```

**Read a file:**
```bash
go run cmd/client/main.go -cmd read -remote /myfile.txt -output retrieved.txt -master 127.0.0.1:50051
```

**Delete a file:**
```bash
go run cmd/client/main.go -cmd delete -remote /myfile.txt -master 127.0.0.1:50051
```

### Interactive REPL Client

For a more interactive experience:

```bash
go run cmd/interactive/main.go -master 127.0.0.1:50051
```

Available commands in REPL:
- `create <filename>` - Create a new file
- `append <filename> <data>` - Append data to a file
- `read <filename>` - Read and display file contents
- `delete <filename>` - Delete a file
- `list` - List all files (if implemented)
- `exit` or `quit` - Exit the REPL

## Running Benchmarks

The project includes comprehensive benchmarking tools for performance analysis.

### Latency Benchmarks

```bash
go run benchmarks/run_benchmarks.go -type latency -master 127.0.0.1:50051
```

Measures operation latency for:
- File creation
- Append operations (various sizes)
- Read operations

### Throughput Benchmarks

```bash
go run benchmarks/run_benchmarks.go -type throughput -master 127.0.0.1:50051
```

Measures system throughput under concurrent load:
- Concurrent append operations
- Data transfer rates
- System scalability

### Benchmark Options

```bash
go run benchmarks/run_benchmarks.go \
  -type latency \
  -master 127.0.0.1:50051 \
  -iterations 100 \
  -output results.csv
```

**Options:**
- `-type`: Benchmark type (`latency` or `throughput`)
- `-master`: Master server address
- `-iterations`: Number of test iterations
- `-output`: Output CSV file for results

### Visualizing Results

```bash
python3 benchmarks/plot_benchmarks.py results.csv
```

## Configuration

Edit `configs/config.yaml` to customize system parameters:

```yaml
master:
  host: "127.0.0.1"
  port: 50051

chunkserver:
  base_port: 6000
  data_dir: "data"
  heartbeat_interval_sec: 5

chunk:
  size_bytes: 67108864  # 64MB
  replication: 3

lease:
  duration_sec: 60
```

## Architecture Overview

### System Components

1. **Master Server**
   - Maintains file system metadata (file-to-chunk mappings)
   - Manages chunk location information
   - Coordinates lease grants for primary replicas
   - Performs heartbeat-based failure detection
   - Handles garbage collection

2. **ChunkServers**
   - Store 64MB chunks of file data
   - Handle read/write/append operations
   - Replicate data to secondary servers
   - Report status via heartbeat messages
   - Maintain persistent state for crash recovery

3. **Clients**
   - Contact master for metadata
   - Directly communicate with chunk servers for data operations
   - Handle retries and error recovery

### Key Workflows

#### File Creation
1. Client requests file creation from master
2. Master creates metadata entry
3. Returns success to client

#### Append Operation (Exactly-Once)
1. Client requests chunk locations from master
2. Master grants primary lease to one replica
3. Client sends data to all replicas (pipelined)
4. **Phase 1**: Client requests primary to append
5. Primary assigns offset and forwards to secondaries
6. Secondaries prepare append and respond
7. **Phase 2**: Primary commits if all replicas ready
8. Client receives offset and completion status
9. Idempotency tracking prevents duplicate appends

#### Read Operation
1. Client requests chunk locations from master
2. Client contacts nearest chunk server
3. ChunkServer returns requested data

## Testing

### Unit Tests

Run all unit tests:

```bash
go test ./...
```

### Exactly-Once Semantics Tests

```bash
go test ./internal/chunkserver -v -run TestExactlyOnce
```

### Primary Crash Tests

```bash
go test ./internal/chunkserver -v -run TestPrimaryCrashBlocking
```

### Detailed Test Output

```bash
go test ./internal/chunkserver -v
```

## Performance Characteristics

Based on benchmark results (see `REPORT.md` for detailed analysis):

- **Append Latency**: ~2-5ms for small appends (<1KB)
- **Large Append Latency**: ~50-100ms for 10MB appends
- **Throughput**: Scales linearly with number of chunk servers
- **Replication Overhead**: ~2-3x latency compared to non-replicated writes
- **Recovery Time**: <1 second for chunk server crash recovery

## Consistency Guarantees

- **Exactly-Once Append**: Each append operation is applied exactly once, even with retries
- **Sequential Consistency**: Appends are applied in the order they are committed
- **Replica Consistency**: All replicas contain identical data after successful commits
- **Crash Recovery**: System recovers to consistent state after failures

## Troubleshooting

### Common Issues

**Port Already in Use:**
```bash
# Check which process is using the port
netstat -ano | findstr :50051
# Kill the process or use a different port
```

**ChunkServers Not Connecting:**
- Verify master address is correct
- Check firewall settings
- Ensure master is running before starting chunk servers

**Operation Timeouts:**
- Increase timeout values in configuration
- Check network connectivity
- Verify chunk servers are healthy (check logs)

### Debug Mode

Enable detailed logging:
```bash
go run cmd/master/main.go -loglevel DEBUG
go run cmd/chunkserver/main.go -loglevel DEBUG
```

## Additional Resources

- **Technical Report**: See [REPORT.md](REPORT.md) for comprehensive system documentation
- **Presentation**: See [PPT.pdf](PPT.pdf) for visual overview

## Implementation Highlights

### Exactly-Once Append Semantics

The system implements a modified two-phase commit protocol with:
- **Operation IDs**: Unique identifiers for each append request
- **Idempotency Tracking**: ChunkServers remember completed operations
- **State Persistence**: Append states survive crashes
- **Retry Handling**: Clients can safely retry failed operations

### Fault Tolerance

- **Heartbeat Monitoring**: Master detects failed chunk servers within seconds
- **Automatic Re-replication**: System restores replication factor after failures
- **Master Recovery**: WAL enables master restart without data loss
- **ChunkServer Recovery**: Persistent state enables crash recovery

### Performance Optimizations

- **Data Pipelining**: Data flows through replicas in a chain
- **Batched Heartbeats**: Reduces network overhead
- **Lease Caching**: Reduces master load for repeated writes
- **Asynchronous Replication**: Non-blocking secondary writes