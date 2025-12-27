package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	pb "ds-gfs-append/api/proto"
	cli "ds-gfs-append/internal/client"
	"ds-gfs-append/internal/config"
)

var (
	masterAddr string
	cfg        *config.Config
)

func main() {
	cfgPath := flag.String("config", "configs/config.yaml", "config file path")
	masterOverride := flag.String("master", "", "master address (override config)")
	flag.Parse()

	var err error
	cfg, err = config.Load(*cfgPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Warning: Could not load config: %v\n", err)
		cfg = &config.Config{}
	}

	masterAddr = *masterOverride
	if masterAddr == "" {
		masterAddr = cfg.MasterAddress()
	}
	if masterAddr == "" {
		masterAddr = "127.0.0.1:50051" // default
	}

	printWelcome()
	runREPL()
}

func printWelcome() {
	fmt.Println("╔═══════════════════════════════════════════════════════════╗")
	fmt.Println("║          GFS Interactive Client - Welcome!                ║")
	fmt.Println("╚═══════════════════════════════════════════════════════════╝")
	fmt.Printf("Connected to Master: %s\n\n", masterAddr)
	printHelp()
}

func printHelp() {
	fmt.Println("Available commands:")
	fmt.Println("  create <filename>               - Create a new file")
	fmt.Println("  append <filename> <local_file>  - Append data from local file")
	fmt.Println("  appendstr <filename> <data>     - Append literal string data")
	fmt.Println("  delete <filename>               - Delete a file")
	fmt.Println("  write <filename> <offset> <local_file> - Write file content at offset")
	fmt.Println("  writestr <filename> <offset> <data> - Write literal string at offset")
	fmt.Println("  read <filename> <offset> <length> [dest] - Read data range")
	fmt.Println("  readall <filename> [dest]       - Read entire file contents")
	fmt.Println("  rename <old> <new>              - Rename a file")
	fmt.Println("  ls                              - List all files")
	fmt.Println("  list <filename>                 - Show file chunks info")
	fmt.Println("  help                            - Show this help message")
	fmt.Println("  quit / exit                     - Exit the client")
	fmt.Println()
}

func runREPL() {
	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Print("gfs> ")
		if !scanner.Scan() {
			break
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		parts := strings.Fields(line)
		if len(parts) == 0 {
			continue
		}

		cmd := strings.ToLower(parts[0])
		args := parts[1:]

		switch cmd {
		case "create":
			handleCreate(args)
		case "append":
			handleAppend(args)
		case "appendstr":
			handleAppendString(args)
		case "write":
			handleWrite(args)
		case "writestr":
			handleWriteString(args)
		case "read":
			handleRead(args)
		case "readall":
			handleReadAll(args)
		case "delete":
			handleDelete(args)
		case "rename":
			handleRename(args)
		case "ls":
			handleLs()
		case "list":
			handleList(args)
		case "help", "?":
			printHelp()
		case "quit", "exit":
			fmt.Println("Goodbye!")
			return
		default:
			fmt.Printf("Unknown command: %s. Type 'help' for available commands.\n", cmd)
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "Error reading input: %v\n", err)
	}
}

func handleCreate(args []string) {
	if len(args) < 1 {
		fmt.Println("Usage: create <filename>")
		return
	}

	filename := args[0]
	fmt.Printf("Creating file '%s'...\n", filename)

	if err := cli.CreateFile(masterAddr, filename); err != nil {
		if strings.Contains(err.Error(), "file exists") {
			fmt.Printf("✗ Error: File '%s' already exists\n", filename)
		} else {
			fmt.Printf("✗ Error creating file: %v\n", err)
		}
		return
	}

	fmt.Printf("✓ File '%s' created successfully\n", filename)
}

func handleAppend(args []string) {
	if len(args) < 2 {
		fmt.Println("Usage: append <filename> <local_file>")
		return
	}

	filename := args[0]
	localFile := args[1]

	// Check if local file exists
	if _, err := os.Stat(localFile); os.IsNotExist(err) {
		fmt.Printf("✗ Error: Local file '%s' not found\n", localFile)
		return
	}

	fmt.Printf("Appending data from '%s' to '%s'...\n", localFile, filename)

	parts, err := cli.ReadFileChunks(localFile, cfg.Chunk.SizeBytes)
	if err != nil {
		fmt.Printf("✗ Error reading local file: %v\n", err)
		return
	}

	if len(parts) == 0 {
		fmt.Println("✗ Error: Local file is empty")
		return
	}

	totalBytes, err := appendDataParts(filename, parts)
	if err != nil {
		if strings.Contains(err.Error(), "file not found") {
			fmt.Printf("✗ Error: File '%s' does not exist. Create it first with 'create %s'\n", filename, filename)
		} else {
			fmt.Printf("✗ %v\n", err)
		}
		return
	}

	fmt.Printf("✓ Successfully appended %d bytes to '%s'\n", totalBytes, filename)
}

func handleAppendString(args []string) {
	if len(args) < 2 {
		fmt.Println("Usage: appendstr <filename> <data>")
		return
	}
	filename := args[0]
	payload := strings.Join(args[1:], " ")
	payload = stripQuotes(payload)
	if payload == "" {
		fmt.Println("✗ Error: data string must not be empty")
		return
	}

	fmt.Printf("Appending literal data to '%s'...\n", filename)
	totalBytes, err := appendDataParts(filename, [][]byte{[]byte(payload)})
	if err != nil {
		if strings.Contains(err.Error(), "file not found") {
			fmt.Printf("✗ Error: File '%s' does not exist. Create it first with 'create %s'\n", filename, filename)
		} else {
			fmt.Printf("✗ %v\n", err)
		}
		return
	}

	fmt.Printf("✓ Successfully appended %d bytes to '%s'\n", totalBytes, filename)
}

func handleWrite(args []string) {
	if len(args) < 3 {
		fmt.Println("Usage: write <filename> <offset> <local_file>")
		return
	}

	filename := args[0]
	offset, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil || offset < 0 {
		fmt.Println("✗ Error: offset must be a non-negative integer")
		return
	}
	srcPath := args[2]

	data, err := os.ReadFile(srcPath)
	if err != nil {
		fmt.Printf("✗ Error reading local file '%s': %v\n", srcPath, err)
		return
	}
	if len(data) == 0 {
		fmt.Println("✗ Error: Local file is empty")
		return
	}

	totalWritten, err := writeData(filename, offset, data)
	if err != nil {
		if strings.Contains(err.Error(), "file not found") {
			fmt.Printf("✗ Error: File '%s' does not exist\n", filename)
		} else {
			fmt.Printf("✗ %v\n", err)
		}
		return
	}

	fmt.Printf("✓ Wrote %d bytes to '%s' starting at offset %d\n", totalWritten, filename, offset)
}

func handleWriteString(args []string) {
	if len(args) < 3 {
		fmt.Println("Usage: writestr <filename> <offset> <data>")
		return
	}

	filename := args[0]
	offset, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil || offset < 0 {
		fmt.Println("✗ Error: offset must be a non-negative integer")
		return
	}
	payload := strings.Join(args[2:], " ")
	payload = stripQuotes(payload)
	if payload == "" {
		fmt.Println("✗ Error: data string must not be empty")
		return
	}

	totalWritten, err := writeData(filename, offset, []byte(payload))
	if err != nil {
		if strings.Contains(err.Error(), "file not found") {
			fmt.Printf("✗ Error: File '%s' does not exist\n", filename)
		} else {
			fmt.Printf("✗ %v\n", err)
		}
		return
	}

	fmt.Printf("✓ Wrote %d bytes to '%s' starting at offset %d\n", totalWritten, filename, offset)
}

func handleRead(args []string) {
	if len(args) < 3 {
		fmt.Println("Usage: read <filename> <offset> <length> [destination_file]")
		return
	}

	filename := args[0]
	offset, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil || offset < 0 {
		fmt.Println("✗ Error: offset must be a non-negative integer")
		return
	}
	length, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil || length <= 0 {
		fmt.Println("✗ Error: length must be a positive integer")
		return
	}
	dest := ""
	if len(args) >= 4 {
		dest = args[3]
	}

	data, err := readFileRange(masterAddr, filename, offset, length)
	if err != nil {
		if strings.Contains(err.Error(), "file not found") {
			fmt.Printf("✗ Error: File '%s' does not exist\n", filename)
		} else {
			fmt.Printf("✗ Error reading file: %v\n", err)
		}
		return
	}

	if dest != "" {
		if writeErr := os.WriteFile(dest, data, 0644); writeErr != nil {
			fmt.Printf("✗ Error writing destination file: %v\n", writeErr)
			return
		}
		fmt.Printf("✓ Read %d bytes into '%s'\n", len(data), dest)
		if int64(len(data)) != length {
			fmt.Printf("  (Warning: requested %d bytes, received %d)\n", length, len(data))
		}
		return
	}

	fmt.Printf("✓ Read %d bytes\n", len(data))
	if int64(len(data)) != length {
		fmt.Printf("  (Warning: requested %d bytes, received %d)\n", length, len(data))
	}
	if len(data) == 0 {
		fmt.Println("(no data)")
		return
	}
	fmt.Println(string(data))
}

func handleReadAll(args []string) {
	if len(args) < 1 {
		fmt.Println("Usage: readall <filename> [destination_file]")
		return
	}

	filename := args[0]
	dest := ""
	if len(args) >= 2 {
		dest = args[1]
	}

	data, err := readEntireFile(masterAddr, filename)
	if err != nil {
		if strings.Contains(err.Error(), "file not found") {
			fmt.Printf("✗ Error: File '%s' does not exist\n", filename)
		} else {
			fmt.Printf("✗ Error reading file: %v\n", err)
		}
		return
	}

	if dest != "" {
		if writeErr := os.WriteFile(dest, data, 0644); writeErr != nil {
			fmt.Printf("✗ Error writing destination file: %v\n", writeErr)
			return
		}
		fmt.Printf("✓ Read %d bytes into '%s'\n", len(data), dest)
		return
	}

	fmt.Printf("✓ Read %d bytes\n", len(data))
	if len(data) == 0 {
		fmt.Println("(no data)")
		return
	}
	fmt.Println(string(data))
}

func appendDataParts(filename string, parts [][]byte) (int, error) {
	if len(parts) == 0 {
		return 0, fmt.Errorf("Error: no data to append")
	}

	meta, err := cli.GetFileChunks(masterAddr, filename)
	if err != nil {
		return 0, fmt.Errorf("Error getting file info: %w", err)
	}

	var chunk *pb.ChunkReplica
	if len(meta.Chunks) == 0 {
		fmt.Println("  → Allocating first chunk...")
		allocResp, err := cli.AllocateChunk(masterAddr, filename)
		if err != nil {
			return 0, fmt.Errorf("Error allocating chunk: %w", err)
		}
		fmt.Printf("  → Chunk allocated: %s with %d replicas\n", allocResp.Handle, len(allocResp.Replicas))
		if len(allocResp.Replicas) == 0 {
			return 0, fmt.Errorf("Error: No replicas available for chunk")
		}
		fmt.Println("  → Requesting lease from master...")
		leaseResp, err := cli.RequestLease(masterAddr, allocResp.Handle, allocResp.Replicas[0].Address)
		if err != nil {
			return 0, fmt.Errorf("Error requesting lease: %w", err)
		}
		if !leaseResp.Granted {
			return 0, fmt.Errorf("Error: Lease not granted")
		}
		chunk = &pb.ChunkReplica{
			Handle:              allocResp.Handle,
			Version:             leaseResp.Version,
			Primary:             leaseResp.Primary,
			Secondaries:         leaseResp.Secondaries,
			LeaseExpirationUnix: leaseResp.LeaseExpirationUnix,
		}
		primaryAddr := ""
		if chunk.Primary != nil {
			primaryAddr = chunk.Primary.Address
		}
		fmt.Printf("  → Lease granted: primary=%s, version=%d\n", primaryAddr, chunk.Version)
	} else {
		chunk = meta.Chunks[0]
	}

	primaryAddr, secondaries := extractReplicas(chunk)
	if primaryAddr == "" {
		return 0, fmt.Errorf("Error: No primary replica available")
	}
	replicas := append([]string{primaryAddr}, secondaries...)
	totalBytes := 0
	for idx, part := range parts {
		stamp := time.Now().UnixNano()
		dataID := fmt.Sprintf("%s-data-%d-%d", chunk.Handle, stamp, idx)
		appendID := fmt.Sprintf("%s-append-%d-%d", chunk.Handle, stamp, idx)
		fmt.Printf("  → Pushing data part %d/%d (%d bytes)...\n", idx+1, len(parts), len(part))
		var pushErr error
		dataID, pushErr = cli.PushToReplicas(replicas, dataID, part)
		if pushErr != nil {
			return totalBytes, fmt.Errorf("Error pushing data: %w", pushErr)
		}
		fmt.Println("  → Appending to chunk...")
		resp, appendErr := cli.RetryAppend(primaryAddr, secondaries, chunk.Handle, dataID, appendID, chunk.Version, 3)
		if appendErr != nil {
			return totalBytes, fmt.Errorf("Error appending: %w", appendErr)
		}
		totalBytes += len(part)
		fmt.Printf("  → Part %d appended at offset %d\n", idx+1, resp.Offset)
	}
	return totalBytes, nil
}

func writeData(filename string, offset int64, data []byte) (int, error) {
	meta, err := cli.GetFileChunks(masterAddr, filename)
	if err != nil {
		return 0, fmt.Errorf("Error getting file info: %w", err)
	}

	chunkSize := chunkSizeBytes()
	if chunkSize <= 0 {
		return 0, fmt.Errorf("Error: invalid chunk size configuration")
	}

	remaining := data
	currentOffset := offset
	totalWritten := 0

	for len(remaining) > 0 {
		chunkIdx := int(currentOffset / chunkSize)
		inChunkOffset := currentOffset % chunkSize

		for len(meta.Chunks) <= chunkIdx {
			fmt.Printf("  → Allocating chunk #%d...\n", len(meta.Chunks)+1)
			allocResp, allocErr := cli.AllocateChunk(masterAddr, filename)
			if allocErr != nil {
				return totalWritten, fmt.Errorf("Error allocating chunk: %w", allocErr)
			}
			fmt.Printf("    Added chunk %s\n", allocResp.Handle)
			meta, err = cli.GetFileChunks(masterAddr, filename)
			if err != nil {
				return totalWritten, fmt.Errorf("Error refreshing file metadata: %w", err)
			}
		}

		chunk := meta.Chunks[chunkIdx]
		primary, secondaries, version, leaseErr := ensureChunkLease(masterAddr, chunk)
		if leaseErr != nil {
			return totalWritten, fmt.Errorf("Error acquiring lease: %w", leaseErr)
		}

		space := chunkSize - inChunkOffset
		if space <= 0 {
			currentOffset = (int64(chunkIdx) + 1) * chunkSize
			continue
		}

		bytesThisChunk := int64(len(remaining))
		if bytesThisChunk > space {
			bytesThisChunk = space
		}

		payload := remaining[:int(bytesThisChunk)]
		replicas := append([]string{primary}, secondaries...)
		dataID := fmt.Sprintf("%s-%d", chunk.Handle, time.Now().UnixNano())
		fmt.Printf("  → Writing %d bytes to chunk %s (offset %d)...\n", len(payload), chunk.Handle, inChunkOffset)
		dataID, err = cli.PushToReplicas(replicas, dataID, payload)
		if err != nil {
			return totalWritten, fmt.Errorf("Error pushing data: %w", err)
		}

		resp, err := cli.WriteChunk(primary, chunk.Handle, dataID, secondaries, version, inChunkOffset)
		if err != nil {
			return totalWritten, fmt.Errorf("Error writing chunk: %w", err)
		}
		if resp == nil || !resp.Success {
			return totalWritten, fmt.Errorf("Error: primary rejected write for chunk %s", chunk.Handle)
		}

		totalWritten += len(payload)
		remaining = remaining[int(bytesThisChunk):]
		currentOffset += bytesThisChunk
	}

	return totalWritten, nil
}

func readEntireFile(masterAddr, filename string) ([]byte, error) {
	meta, err := cli.GetFileChunks(masterAddr, filename)
	if err != nil {
		return nil, err
	}
	if len(meta.Chunks) == 0 {
		return []byte{}, nil
	}

	var result []byte
	for _, chunk := range meta.Chunks {
		addrs := pickReplicaAddrs(chunk)
		if len(addrs) == 0 {
			return nil, fmt.Errorf("no replicas available for chunk %s", chunk.Handle)
		}
		var data []byte
		var readErr error
		for _, addr := range addrs {
			data, readErr = cli.ReadChunk(addr, chunk.Handle, 0, 0)
			if readErr == nil {
				break
			}
		}
		if readErr != nil {
			return nil, fmt.Errorf("failed to read chunk %s: %w", chunk.Handle, readErr)
		}
		result = append(result, data...)
	}

	return result, nil
}

func stripQuotes(s string) string {
	if len(s) >= 2 {
		last := len(s) - 1
		if (s[0] == '"' && s[last] == '"') || (s[0] == '\'' && s[last] == '\'') {
			return s[1:last]
		}
	}
	return s
}

func handleDelete(args []string) {
	if len(args) < 1 {
		fmt.Println("Usage: delete <filename>")
		return
	}

	filename := args[0]
	fmt.Printf("Deleting file '%s'...\n", filename)

	if err := cli.DeleteFile(masterAddr, filename); err != nil {
		if strings.Contains(err.Error(), "file not found") {
			fmt.Printf("✗ Error: File '%s' does not exist\n", filename)
		} else {
			fmt.Printf("✗ Error deleting file: %v\n", err)
		}
		return
	}

	fmt.Printf("✓ File '%s' deleted successfully\n", filename)
}

func handleRename(args []string) {
	if len(args) < 2 {
		fmt.Println("Usage: rename <old_filename> <new_filename>")
		return
	}

	oldName := args[0]
	newName := args[1]
	if oldName == newName {
		fmt.Println("✗ Error: old and new filenames must differ")
		return
	}

	if err := cli.RenameFile(masterAddr, oldName, newName); err != nil {
		fmt.Printf("✗ Error renaming file: %v\n", err)
		return
	}

	fmt.Printf("✓ Renamed '%s' to '%s'\n", oldName, newName)
}

func handleLs() {
	fmt.Println("Listing files...")
	files, err := cli.ListFiles(masterAddr)
	if err != nil {
		fmt.Printf("✗ Error listing files: %v\n", err)
		return
	}
	if len(files) == 0 {
		fmt.Println("  (no files)")
		return
	}
	for _, name := range files {
		fmt.Printf("  %s\n", name)
	}
}

func handleList(args []string) {
	if len(args) < 1 {
		fmt.Println("Usage: list <filename>")
		return
	}

	filename := args[0]
	fmt.Printf("Getting info for file '%s'...\n", filename)

	meta, err := cli.GetFileChunks(masterAddr, filename)
	if err != nil {
		if strings.Contains(err.Error(), "file not found") {
			fmt.Printf("✗ Error: File '%s' does not exist\n", filename)
		} else {
			fmt.Printf("✗ Error getting file info: %v\n", err)
		}
		return
	}

	if len(meta.Chunks) == 0 {
		fmt.Printf("File '%s' exists but has no chunks yet (empty file)\n", filename)
		return
	}

	fmt.Printf("\nFile: %s\n", filename)
	fmt.Printf("Chunks: %d\n\n", len(meta.Chunks))

	for i, chunk := range meta.Chunks {
		fmt.Printf("Chunk #%d:\n", i+1)
		fmt.Printf("  Handle:  %s\n", chunk.Handle)
		fmt.Printf("  Version: %d\n", chunk.Version)
		if chunk.Primary != nil {
			fmt.Printf("  Primary: %s\n", chunk.Primary.Address)
		} else {
			fmt.Printf("  Primary: (none)\n")
		}
		if len(chunk.Secondaries) > 0 {
			fmt.Printf("  Secondaries:\n")
			for _, sec := range chunk.Secondaries {
				fmt.Printf("    - %s\n", sec.Address)
			}
		} else {
			fmt.Printf("  Secondaries: (none)\n")
		}
		if chunk.LeaseExpirationUnix > 0 {
			expTime := time.Unix(chunk.LeaseExpirationUnix, 0)
			if time.Now().Before(expTime) {
				fmt.Printf("  Lease: Valid until %s\n", expTime.Format("15:04:05"))
			} else {
				fmt.Printf("  Lease: Expired\n")
			}
		}
		fmt.Println()
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

func chunkSizeBytes() int64 {
	if cfg != nil && cfg.Chunk.SizeBytes > 0 {
		return cfg.Chunk.SizeBytes
	}
	return 64 << 20
}

func ensureChunkLease(masterAddr string, chunk *pb.ChunkReplica) (string, []string, int64, error) {
	primary := ""
	if chunk.Primary != nil {
		primary = chunk.Primary.Address
	}
	secondaries := addressesFrom(chunk.Secondaries)
	version := chunk.Version
	renewThreshold := time.Now().Add(2 * time.Second)
	needsLease := primary == "" || chunk.LeaseExpirationUnix == 0 || time.Unix(chunk.LeaseExpirationUnix, 0).Before(renewThreshold)
	if needsLease {
		requester := primary
		if requester == "" && len(secondaries) > 0 {
			requester = secondaries[0]
		}
		resp, err := cli.RequestLease(masterAddr, chunk.Handle, requester)
		if err != nil {
			return "", nil, 0, err
		}
		if !resp.GetGranted() || resp.Primary == nil || resp.Primary.Address == "" {
			return "", nil, 0, fmt.Errorf("lease not granted for chunk %s", chunk.Handle)
		}
		primary = resp.Primary.Address
		secondaries = addressesFrom(resp.Secondaries)
		version = resp.Version
		chunk.Primary = resp.Primary
		chunk.Secondaries = resp.Secondaries
		chunk.Version = resp.Version
		chunk.LeaseExpirationUnix = resp.LeaseExpirationUnix
	}
	if primary == "" {
		return "", nil, 0, fmt.Errorf("no replicas available for chunk %s", chunk.Handle)
	}
	return primary, secondaries, version, nil
}

func addressesFrom(locs []*pb.ChunkLocation) []string {
	out := make([]string, 0, len(locs))
	for _, loc := range locs {
		if loc == nil || loc.Address == "" {
			continue
		}
		out = append(out, loc.Address)
	}
	return out
}

func pickReplicaAddrs(chunk *pb.ChunkReplica) []string {
	addresses := make([]string, 0, 1+len(chunk.Secondaries))
	if chunk.Primary != nil && chunk.Primary.Address != "" {
		addresses = append(addresses, chunk.Primary.Address)
	}
	for _, loc := range chunk.Secondaries {
		if loc == nil || loc.Address == "" {
			continue
		}
		addresses = append(addresses, loc.Address)
	}
	return addresses
}

func readFileRange(masterAddr, filename string, offset, length int64) ([]byte, error) {
	if offset < 0 {
		return nil, fmt.Errorf("offset must be >= 0")
	}
	if length <= 0 {
		return nil, fmt.Errorf("length must be > 0")
	}

	meta, err := cli.GetFileChunks(masterAddr, filename)
	if err != nil {
		return nil, err
	}
	chunkSize := chunkSizeBytes()
	if chunkSize <= 0 {
		return nil, fmt.Errorf("invalid chunk size configuration")
	}

	var result []byte
	remaining := length
	currentOffset := offset

	for remaining > 0 {
		chunkIdx := int(currentOffset / chunkSize)
		if chunkIdx >= len(meta.Chunks) {
			break
		}

		chunk := meta.Chunks[chunkIdx]
		inChunkOffset := currentOffset % chunkSize
		space := chunkSize - inChunkOffset
		if space <= 0 {
			currentOffset = (int64(chunkIdx) + 1) * chunkSize
			continue
		}

		requestLen := remaining
		if requestLen > space {
			requestLen = space
		}

		addrs := pickReplicaAddrs(chunk)
		if len(addrs) == 0 {
			return nil, fmt.Errorf("no replicas available for chunk %s", chunk.Handle)
		}

		var data []byte
		var readErr error
		for _, addr := range addrs {
			data, readErr = cli.ReadChunk(addr, chunk.Handle, inChunkOffset, requestLen)
			if readErr == nil {
				break
			}
		}
		if readErr != nil {
			return nil, fmt.Errorf("failed to read chunk %s: %w", chunk.Handle, readErr)
		}

		result = append(result, data...)
		readLen := int64(len(data))
		if readLen == 0 {
			break
		}

		currentOffset += readLen
		if readLen >= remaining {
			break
		}
		remaining -= readLen
	}

	return result, nil
}
