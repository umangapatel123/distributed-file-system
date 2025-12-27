package chunkserver

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	pb "ds-gfs-append/api/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	defaultChunkSize     int64 = 64 << 20 // 64MB
	defaultBufferEntries       = 128
	leaseRefreshMargin         = 5 * time.Second
)

type bufferEntry struct {
	id   string
	data []byte
}

type ChunkServer struct {
	pb.UnimplementedChunkServerServer

	addr       string
	masterAddr string
	dataDir    string
	chunkSize  int64
	hbInterval time.Duration

	mu            sync.Mutex
	bufferIdx     map[string]*list.Element
	bufferOrder   *list.List
	bufferLimit   int
	chunkSizes    map[string]int64
	chunkVersion  map[string]int64
	leaseCache    map[string]leaseState
	chunkLockMu   sync.Mutex
	chunkLocks    map[string]*sync.Mutex
	appendStateMu sync.Mutex
	appendStates  map[string]map[string]*appendRecordState

	// Persistent state sync
	statePath             string
	stateSyncInterval     time.Duration
	stateStop             chan struct{}
	stateVersion          uint64
	statePersistedVersion uint64
	stateWg               sync.WaitGroup

	// Recovery mechanisms
	preparedTxnMonitor *PreparedTransactionMonitor
}

type leaseState struct {
	expiration time.Time
	version    int64
}

func NewChunkServer(addr, dataDir, masterAddr string, chunkSize int64, hbInterval time.Duration) *ChunkServer {
	if chunkSize <= 0 {
		chunkSize = defaultChunkSize
	}
	cs := &ChunkServer{
		addr:         addr,
		masterAddr:   masterAddr,
		dataDir:      dataDir,
		chunkSize:    chunkSize,
		hbInterval:   hbInterval,
		bufferIdx:    make(map[string]*list.Element),
		bufferOrder:  list.New(),
		bufferLimit:  defaultBufferEntries,
		chunkSizes:   make(map[string]int64),
		chunkVersion: make(map[string]int64),
		leaseCache:   make(map[string]leaseState),
		chunkLocks:   make(map[string]*sync.Mutex),
		appendStates: make(map[string]map[string]*appendRecordState),
	}
	cs.loadExistingChunks()
	cs.initStatePersistence()
	// Initialize recovery mechanisms
	cs.preparedTxnMonitor = NewPreparedTransactionMonitor(cs)

	return cs
}

func (cs *ChunkServer) Run(listenAddr string) error {
	go cs.heartbeatLoop()
	go cs.leaseRefreshLoop()

	// Start prepared transaction monitor
	cs.preparedTxnMonitor.Start()

	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return err
	}
	srv := grpc.NewServer()
	pb.RegisterChunkServerServer(srv, cs)
	log.Printf("chunkserver listening %s", listenAddr)
	return srv.Serve(lis)
}

func (cs *ChunkServer) heartbeatLoop() {
	if cs.masterAddr == "" {
		return
	}
	if err := cs.sendHeartbeat(); err != nil {
		log.Printf("heartbeat failed: %v", err)
	}
	ticker := time.NewTicker(cs.hbInterval)
	defer ticker.Stop()
	for range ticker.C {
		if err := cs.sendHeartbeat(); err != nil {
			log.Printf("heartbeat failed: %v", err)
		}
	}
}

func (cs *ChunkServer) leaseRefreshLoop() {
	if cs.masterAddr == "" {
		return
	}
	interval := cs.hbInterval / 2
	if interval <= 0 {
		interval = 5 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		cs.refreshExpiringLeases()
	}
}

func (cs *ChunkServer) loadExistingChunks() {
	if cs.dataDir == "" {
		return
	}
	if err := os.MkdirAll(cs.dataDir, 0o755); err != nil {
		log.Printf("chunkserver %s: failed to ensure data dir: %v", cs.addr, err)
		return
	}
	entries, err := os.ReadDir(cs.dataDir)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Printf("chunkserver %s: failed to scan data dir: %v", cs.addr, err)
		}
		return
	}
	loaded := 0
	cs.mu.Lock()
	defer cs.mu.Unlock()
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		handle := parseChunkHandle(entry.Name())
		if handle == "" {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			log.Printf("chunkserver %s: stat failed for %s: %v", cs.addr, entry.Name(), err)
			continue
		}
		cs.chunkSizes[handle] = info.Size()
		if _, ok := cs.chunkVersion[handle]; !ok {
			cs.chunkVersion[handle] = 1
		}
		loaded++
	}
	if loaded > 0 {
		log.Printf("chunkserver %s: discovered %d existing chunks", cs.addr, loaded)
	}
}

func parseChunkHandle(name string) string {
	if !strings.HasPrefix(name, "chunk_") || !strings.HasSuffix(name, ".chk") {
		return ""
	}
	h := strings.TrimSuffix(strings.TrimPrefix(name, "chunk_"), ".chk")
	if h == "" {
		return ""
	}
	return h
}

func (cs *ChunkServer) sendHeartbeat() error {
	conn, err := grpcDial(cs.masterAddr)
	if err != nil {
		return err
	}
	defer conn.Close()
	client := pb.NewMasterClient(conn)
	cs.mu.Lock()
	chunks := make([]*pb.HeartBeatChunk, 0, len(cs.chunkSizes))
	for handle, size := range cs.chunkSizes {
		ver := cs.chunkVersion[handle]
		chunks = append(chunks, &pb.HeartBeatChunk{Handle: handle, Version: ver, SizeBytes: size})
	}
	cs.mu.Unlock()
	log.Printf("chunkserver %s: sending heartbeat with %d chunks", cs.addr, len(chunks))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	resp, err := client.HeartBeat(ctx, &pb.HeartBeatRequest{Address: cs.addr, Chunks: chunks})
	if err != nil {
		return err
	}
	commands := resp.GetCommands()
	if len(commands) > 0 {
		log.Printf("chunkserver %s: heartbeat received %d commands", cs.addr, len(commands))
	}
	cs.handleCommands(commands)
	return nil
}

func (cs *ChunkServer) leasesNearingExpiry(margin time.Duration) []string {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	now := time.Now()
	var handles []string
	for handle, lease := range cs.leaseCache {
		if now.Add(margin).After(lease.expiration) {
			handles = append(handles, handle)
		}
	}
	return handles
}

func (cs *ChunkServer) refreshExpiringLeases() {
	if cs.masterAddr == "" {
		return
	}
	handles := cs.leasesNearingExpiry(leaseRefreshMargin)
	for _, handle := range handles {
		if _, err := cs.acquireLease(handle, true); err != nil {
			log.Printf("chunkserver %s: lease refresh failed for chunk %s: %v", cs.addr, handle, err)
			cs.dropLease(handle)
		}
	}
}

func (cs *ChunkServer) dropLease(handle string) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	delete(cs.leaseCache, handle)
}

func (cs *ChunkServer) storeBuffer(dataID string, data []byte) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	if elem, exists := cs.bufferIdx[dataID]; exists {
		entry := elem.Value.(*bufferEntry)
		entry.data = append(entry.data, data...)
		cs.bufferOrder.MoveToFront(elem)
		return
	}
	entry := &bufferEntry{id: dataID, data: append([]byte(nil), data...)}
	elem := cs.bufferOrder.PushFront(entry)
	cs.bufferIdx[dataID] = elem
	for cs.bufferOrder.Len() > cs.bufferLimit {
		tail := cs.bufferOrder.Back()
		if tail == nil {
			break
		}
		be := tail.Value.(*bufferEntry)
		delete(cs.bufferIdx, be.id)
		cs.bufferOrder.Remove(tail)
	}
}

func (cs *ChunkServer) getBuffer(dataID string) ([]byte, error) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	elem, ok := cs.bufferIdx[dataID]
	if !ok {
		return nil, errors.New("data not buffered")
	}
	cs.bufferOrder.MoveToFront(elem)
	data := elem.Value.(*bufferEntry).data
	return append([]byte(nil), data...), nil
}

func (cs *ChunkServer) acquireLease(handle string, force bool) (int64, error) {
	if cs.masterAddr == "" {
		cs.mu.Lock()
		version := cs.chunkVersion[handle]
		cs.mu.Unlock()
		return version, nil
	}

	cs.mu.Lock()
	ls, ok := cs.leaseCache[handle]
	if ok && !force && time.Now().Before(ls.expiration) {
		version := ls.version
		cs.mu.Unlock()
		return version, nil
	}
	cs.mu.Unlock()

	log.Printf("chunkserver %s: requesting lease for chunk %s", cs.addr, handle)
	conn, err := grpcDial(cs.masterAddr)
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	client := pb.NewMasterClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	resp, err := client.RequestLease(ctx, &pb.RequestLeaseRequest{ChunkHandle: handle, Requester: cs.addr})
	if err != nil {
		return 0, err
	}
	if !resp.Granted {
		log.Printf("chunkserver %s: lease denied for chunk %s", cs.addr, handle)
		return 0, errors.New("lease not granted")
	}
	exp := time.Unix(resp.LeaseExpirationUnix, 0)
	cs.mu.Lock()
	cs.leaseCache[handle] = leaseState{expiration: exp, version: resp.Version}
	cs.mu.Unlock()
	cs.setChunkVersion(handle, resp.Version)
	log.Printf("chunkserver %s: lease granted chunk=%s version=%d expires=%s", cs.addr, handle, resp.Version, exp.Format(time.RFC3339))
	return resp.Version, nil
}

func (cs *ChunkServer) ensureLease(handle string) (int64, error) {
	return cs.acquireLease(handle, false)
}

func (cs *ChunkServer) nextOffset(handle string, dataLen int) (int64, error) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	size := cs.chunkSizes[handle]
	if size == 0 {
		s, err := getSize(cs.dataDir, handle)
		if err != nil {
			return 0, err
		}
		size = s
	}
	if size+int64(dataLen) > cs.chunkSize {
		return 0, fmt.Errorf("chunk %s full", handle)
	}
	offset := size
	cs.chunkSizes[handle] = size + int64(dataLen)
	cs.markStateDirty()
	return offset, nil
}

func (cs *ChunkServer) recordWrite(handle string, size int) {
	cs.mu.Lock()
	cs.chunkSizes[handle] += int64(size)
	cs.mu.Unlock()
	cs.markStateDirty()
}

func grpcDial(addr string) (*grpc.ClientConn, error) {
	return grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
}

func (cs *ChunkServer) handleCommands(cmds []*pb.HeartBeatCommand) {
	if len(cmds) == 0 {
		return
	}
	for _, cmd := range cmds {
		switch cmd.GetType() {
		case pb.CommandType_COMMAND_DELETE_CHUNK:
			handle := cmd.GetChunk()
			if handle == "" {
				continue
			}
			if err := cs.deleteChunk(handle); err != nil {
				log.Printf("chunkserver %s: failed to delete chunk %s: %v", cs.addr, handle, err)
			}
		case pb.CommandType_COMMAND_REPLICATE_CHUNK:
			handle := cmd.GetChunk()
			target := cmd.GetTarget()
			if handle == "" || target == "" {
				continue
			}
			if err := cs.replicateChunk(handle, target); err != nil {
				log.Printf("chunkserver %s: replicate chunk=%s to %s failed: %v", cs.addr, handle, target, err)
			}
		default:
			// Unsupported command types can be ignored for now.
		}
	}
}

const replicateChunkBufferSize = 1 << 20

func (cs *ChunkServer) replicateChunk(handle, target string) error {
	lock := cs.getChunkLock(handle)
	lock.Lock()
	defer lock.Unlock()

	size, err := cs.currentChunkSize(handle)
	if err != nil {
		return err
	}
	data, err := readAt(cs.dataDir, handle, 0, size)
	if err != nil {
		return err
	}
	cs.mu.Lock()
	version := cs.chunkVersion[handle]
	cs.mu.Unlock()
	if version == 0 {
		version = 1
	}

	conn, err := grpcDial(target)
	if err != nil {
		return err
	}
	defer conn.Close()
	client := pb.NewChunkServerClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream, err := client.PushData(ctx)
	if err != nil {
		return err
	}
	for offset := 0; offset < len(data); offset += replicateChunkBufferSize {
		end := offset + replicateChunkBufferSize
		if end > len(data) {
			end = len(data)
		}
		if err := stream.Send(&pb.PushDataRequest{Data: data[offset:end]}); err != nil {
			return err
		}
	}
	pushResp, err := stream.CloseAndRecv()
	if err != nil {
		return err
	}
	writeCtx, cancelWrite := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelWrite()
	writeResp, err := client.WriteChunk(writeCtx, &pb.WriteChunkRequest{
		ChunkHandle:  handle,
		DataId:       pushResp.GetDataId(),
		ChunkVersion: version,
		Offset:       0,
		BypassLease:  true,
	})
	if err != nil {
		return err
	}
	if writeResp == nil || !writeResp.Success {
		return fmt.Errorf("write rejected by %s", target)
	}
	log.Printf("chunkserver %s: replicated chunk=%s to %s version=%d", cs.addr, handle, target, version)
	return nil
}

func (cs *ChunkServer) deleteChunk(handle string) error {
	cs.mu.Lock()
	delete(cs.chunkSizes, handle)
	delete(cs.chunkVersion, handle)
	delete(cs.leaseCache, handle)
	cs.mu.Unlock()
	cs.markStateDirty()
	if err := deleteChunkFile(cs.dataDir, handle); err != nil {
		return err
	}
	log.Printf("chunkserver %s: deleted chunk %s", cs.addr, handle)
	return nil
}

func (cs *ChunkServer) currentChunkSize(handle string) (int64, error) {
	cs.mu.Lock()
	size, ok := cs.chunkSizes[handle]
	cs.mu.Unlock()
	if ok {
		return size, nil
	}
	sizeOnDisk, err := getSize(cs.dataDir, handle)
	if err != nil {
		return 0, err
	}
	cs.setChunkSize(handle, sizeOnDisk)
	return sizeOnDisk, nil
}

func (cs *ChunkServer) setChunkSize(handle string, size int64) {
	cs.mu.Lock()
	cs.chunkSizes[handle] = size
	cs.mu.Unlock()
	cs.markStateDirty()
}

func (cs *ChunkServer) setChunkVersion(handle string, version int64) {
	cs.mu.Lock()
	cs.chunkVersion[handle] = version
	cs.mu.Unlock()
	cs.markStateDirty()
}

// Shutdown gracefully shuts down the chunk server
func (cs *ChunkServer) Shutdown() {
	if cs.preparedTxnMonitor != nil {
		cs.preparedTxnMonitor.Stop()
	}
	if cs.stateStop != nil {
		close(cs.stateStop)
		cs.stateWg.Wait()
	}
	cs.flushStateIfDirty()
}
