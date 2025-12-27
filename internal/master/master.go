package master

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	pb "ds-gfs-append/api/proto"

	"google.golang.org/grpc"
)

type Config struct {
	ReplicationFactor int
	LeaseDuration     time.Duration
}

func DefaultConfig() Config {
	return Config{ReplicationFactor: 3, LeaseDuration: 60 * time.Second}
}

const replicaLivenessTimeout = 10 * time.Second

type MasterServer struct {
	pb.UnimplementedMasterServer

	meta   *MasterMetadata
	log    *OperationLog
	leases *LeaseManager

	replicationFactor int
	leaseDuration     time.Duration

	handleCounter uint64

	commandMu    sync.Mutex
	commandQueue map[string][]*pb.HeartBeatCommand

	gcMu          sync.Mutex
	garbage       map[string]time.Time
	gcInterval    time.Duration
	gcGracePeriod time.Duration

	metaDumpPath string
	metaDumpMu   sync.Mutex
}

func NewMasterServer(logPath string, metaPath string, cfg Config) (*MasterServer, error) {
	if cfg.ReplicationFactor <= 0 {
		cfg.ReplicationFactor = 3
	}
	if cfg.LeaseDuration <= 0 {
		cfg.LeaseDuration = 60 * time.Second
	}

	ol, err := NewOperationLog(logPath)
	if err != nil {
		return nil, err
	}
	ms := &MasterServer{
		meta:              NewMasterMetadata(),
		log:               ol,
		leases:            NewLeaseManager(),
		replicationFactor: cfg.ReplicationFactor,
		leaseDuration:     cfg.LeaseDuration,
		commandQueue:      make(map[string][]*pb.HeartBeatCommand),
		garbage:           make(map[string]time.Time),
		gcInterval:        30 * time.Second,
		gcGracePeriod:     120 * time.Second,
		metaDumpPath:      metaPath,
	}
	_ = ol.Replay(logPath, ms.meta)
	ms.rebuildGarbageCandidates()
	if metaPath != "" {
		log.Printf("master: metadata snapshots will be written to %s", metaPath)
	}
	go ms.gcLoop()
	rand.Seed(time.Now().UnixNano())
	if metaPath != "" {
		ms.dumpMetadata()
	}
	return ms, nil
}

func (m *MasterServer) Close() error {
	if m.log != nil {
		return m.log.Close()
	}
	return nil
}

func (m *MasterServer) markChunkForGC(handle string) {
	m.gcMu.Lock()
	defer m.gcMu.Unlock()
	if _, exists := m.garbage[handle]; !exists {
		m.garbage[handle] = time.Now()
		log.Printf("master: chunk %s marked for garbage collection", handle)
	}
}

func (m *MasterServer) gcLoop() {
	ticker := time.NewTicker(m.gcInterval)
	defer ticker.Stop()
	for range ticker.C {
		m.collectGarbage()
	}
}

func (m *MasterServer) collectGarbage() {
	now := time.Now()
	var ready []string
	m.gcMu.Lock()
	for handle, marked := range m.garbage {
		if now.Sub(marked) >= m.gcGracePeriod {
			ready = append(ready, handle)
			delete(m.garbage, handle)
		}
	}
	m.gcMu.Unlock()

	for _, handle := range ready {
		locations := m.meta.GetChunkLocations(handle)
		if len(locations) == 0 {
			m.meta.RemoveChunk(handle)
			m.leases.Revoke(handle)
			continue
		}
		m.enqueueDeleteCommand(handle, locations)
		m.meta.RemoveChunk(handle)
		m.leases.Revoke(handle)
	}
	if len(ready) > 0 {
		m.dumpMetadataAsync()
	}
}

func (m *MasterServer) enqueueDeleteCommand(handle string, locations []string) {
	m.commandMu.Lock()
	defer m.commandMu.Unlock()
	for _, addr := range locations {
		cmd := &pb.HeartBeatCommand{
			Type:  pb.CommandType_COMMAND_DELETE_CHUNK,
			Chunk: handle,
		}
		m.commandQueue[addr] = append(m.commandQueue[addr], cmd)
		log.Printf("master: scheduled delete of chunk %s on %s", handle, addr)
	}
}

func (m *MasterServer) dequeueCommands(addr string) []*pb.HeartBeatCommand {
	m.commandMu.Lock()
	defer m.commandMu.Unlock()
	cmds := m.commandQueue[addr]
	if len(cmds) == 0 {
		return nil
	}
	// Return a copy to avoid data races if caller modifies slice.
	result := make([]*pb.HeartBeatCommand, len(cmds))
	copy(result, cmds)
	delete(m.commandQueue, addr)
	return result
}

func (m *MasterServer) rebuildGarbageCandidates() {
	m.meta.mu.RLock()
	referenced := make(map[string]struct{})
	for _, chunks := range m.meta.FileMap {
		for _, handle := range chunks {
			referenced[handle] = struct{}{}
		}
	}
	var orphans []string
	for handle := range m.meta.ChunkMap {
		if _, ok := referenced[handle]; !ok {
			orphans = append(orphans, handle)
		}
	}
	m.meta.mu.RUnlock()
	for _, handle := range orphans {
		m.markChunkForGC(handle)
	}
}

func (m *MasterServer) newChunkHandle() string {
	id := atomic.AddUint64(&m.handleCounter, 1)
	return fmt.Sprintf("ch-%d-%d", time.Now().UnixNano(), id)
}

func (m *MasterServer) dumpMetadataAsync() {
	if m.metaDumpPath == "" {
		return
	}
	go m.dumpMetadata()
}

type chunkSnapshot struct {
	Version         int64     `json:"version"`
	Primary         string    `json:"primary"`
	Secondaries     []string  `json:"secondaries"`
	LeaseExpiration time.Time `json:"lease_expiration"`
}

type replicaSnapshot struct {
	LastHeartbeat time.Time        `json:"last_heartbeat"`
	Chunks        map[string]int64 `json:"chunks"`
}

type metadataSnapshot struct {
	Timestamp      time.Time                  `json:"timestamp"`
	Files          map[string][]string        `json:"files"`
	Chunks         map[string]chunkSnapshot   `json:"chunks"`
	ChunkLocations map[string][]string        `json:"chunk_locations"`
	Replicas       map[string]replicaSnapshot `json:"replicas"`
	StaleReplicas  map[string][]string        `json:"stale_replicas"`
	Garbage        []string                   `json:"garbage"`
}

func (m *MasterServer) dumpMetadata() {
	if m.metaDumpPath == "" {
		return
	}

	snapshot := metadataSnapshot{Timestamp: time.Now().UTC()}

	m.meta.mu.RLock()
	if len(m.meta.FileMap) > 0 {
		snapshot.Files = make(map[string][]string, len(m.meta.FileMap))
		for name, chunks := range m.meta.FileMap {
			snapshot.Files[name] = append([]string(nil), chunks...)
		}
	}
	if len(m.meta.ChunkMap) > 0 {
		snapshot.Chunks = make(map[string]chunkSnapshot, len(m.meta.ChunkMap))
		for handle, info := range m.meta.ChunkMap {
			snapshot.Chunks[handle] = chunkSnapshot{
				Version:         info.Version,
				Primary:         info.Primary,
				Secondaries:     append([]string(nil), info.Secondaries...),
				LeaseExpiration: info.LeaseExpiration,
			}
		}
	}
	if len(m.meta.ChunkLocations) > 0 {
		snapshot.ChunkLocations = make(map[string][]string, len(m.meta.ChunkLocations))
		for handle, locs := range m.meta.ChunkLocations {
			addresses := make([]string, 0, len(locs))
			for addr := range locs {
				addresses = append(addresses, addr)
			}
			sort.Strings(addresses)
			snapshot.ChunkLocations[handle] = addresses
		}
	}
	if len(m.meta.Replicas) > 0 {
		snapshot.Replicas = make(map[string]replicaSnapshot, len(m.meta.Replicas))
		for addr, rs := range m.meta.Replicas {
			chunkVersions := make(map[string]int64, len(rs.Chunks))
			for handle, version := range rs.Chunks {
				chunkVersions[handle] = version
			}
			snapshot.Replicas[addr] = replicaSnapshot{LastHeartbeat: rs.LastHeartbeat, Chunks: chunkVersions}
		}
	}
	if len(m.meta.StaleReplicas) > 0 {
		snapshot.StaleReplicas = make(map[string][]string, len(m.meta.StaleReplicas))
		for handle, bucket := range m.meta.StaleReplicas {
			addresses := make([]string, 0, len(bucket))
			for addr := range bucket {
				addresses = append(addresses, addr)
			}
			sort.Strings(addresses)
			snapshot.StaleReplicas[handle] = addresses
		}
	}
	m.meta.mu.RUnlock()

	m.gcMu.Lock()
	if len(m.garbage) > 0 {
		snapshot.Garbage = make([]string, 0, len(m.garbage))
		for handle := range m.garbage {
			snapshot.Garbage = append(snapshot.Garbage, handle)
		}
		sort.Strings(snapshot.Garbage)
	}
	m.gcMu.Unlock()

	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		log.Printf("master: failed to marshal metadata snapshot: %v", err)
		return
	}

	tmpPath := m.metaDumpPath + ".tmp"

	m.metaDumpMu.Lock()
	defer m.metaDumpMu.Unlock()

	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		log.Printf("master: failed to write metadata snapshot: %v", err)
		return
	}
	if err := os.Rename(tmpPath, m.metaDumpPath); err != nil {
		log.Printf("master: failed to persist metadata snapshot: %v", err)
		return
	}
}

func (m *MasterServer) ListFiles(ctx context.Context, req *pb.ListFilesRequest) (*pb.ListFilesResponse, error) {
	m.meta.mu.RLock()
	files := make([]string, 0, len(m.meta.FileMap))
	for name := range m.meta.FileMap {
		files = append(files, name)
	}
	m.meta.mu.RUnlock()
	sort.Strings(files)
	log.Printf("master: list files requested (%d files)", len(files))
	return &pb.ListFilesResponse{Filenames: files}, nil
}

func (m *MasterServer) GetFileChunksInfo(ctx context.Context, req *pb.GetFileChunksInfoRequest) (*pb.GetFileChunksInfoResponse, error) {
	m.meta.mu.RLock()
	handles, exists := m.meta.FileMap[req.Filename]
	m.meta.mu.RUnlock()

	if !exists {
		return nil, errors.New("file not found")
	}
	log.Printf("master: get chunks info for %s (chunks=%d)", req.Filename, len(handles))
	resp := &pb.GetFileChunksInfoResponse{}
	// Return empty list if file has no chunks yet
	if len(handles) == 0 {
		return resp, nil
	}
	for _, handle := range handles {
		now := time.Now()
		liveLocs := m.meta.HealthyChunkLocations(handle, now.Add(-replicaLivenessTimeout))
		leasePrimary, _, expiration, hasLease := m.meta.GetLease(handle)
		leaseValid := hasLease && leasePrimary != "" && now.Before(expiration) && contains(liveLocs, leasePrimary)
		preferred := ""
		if leaseValid {
			preferred = leasePrimary
		}
		primary, secondaries := choosePrimary(liveLocs, preferred)
		ci := m.meta.EnsureChunk(handle)
		chunk := &pb.ChunkReplica{Handle: handle, Version: ci.Version}
		if primary != "" {
			chunk.Primary = &pb.ChunkLocation{Address: primary}
		}
		for _, sec := range secondaries {
			chunk.Secondaries = append(chunk.Secondaries, &pb.ChunkLocation{Address: sec})
		}
		if leaseValid && primary == leasePrimary {
			chunk.LeaseExpirationUnix = expiration.Unix()
		}
		resp.Chunks = append(resp.Chunks, chunk)
	}
	return resp, nil
}

func (m *MasterServer) CreateFile(ctx context.Context, req *pb.CreateFileRequest) (*pb.Status, error) {
	m.meta.mu.Lock()
	if _, exists := m.meta.FileMap[req.Filename]; exists {
		m.meta.mu.Unlock()
		log.Printf("master: create file %s failed - already exists", req.Filename)
		return &pb.Status{Code: 1, Message: "file already exists"}, errors.New("file exists")
	}
	m.meta.FileMap[req.Filename] = []string{} // Empty chunk list
	m.meta.mu.Unlock()
	log.Printf("master: create file %s", req.Filename)
	if err := m.log.Append(opRecord{Op: "create", File: req.Filename, ChunkList: []string{}}); err != nil {
		return &pb.Status{Code: 1, Message: err.Error()}, err
	}
	m.dumpMetadataAsync()
	return &pb.Status{Code: 0, Message: "ok"}, nil
}

func (m *MasterServer) DeleteFile(ctx context.Context, req *pb.DeleteFileRequest) (*pb.Status, error) {
	chunks, removed := m.meta.RemoveFile(req.Filename)
	if !removed {
		log.Printf("master: delete file %s failed - not found", req.Filename)
		return &pb.Status{Code: 1, Message: "file not found"}, errors.New("file not found")
	}
	log.Printf("master: delete file %s (chunks=%d)", req.Filename, len(chunks))
	for _, handle := range chunks {
		m.markChunkForGC(handle)
	}
	if err := m.log.Append(opRecord{Op: "delete", File: req.Filename, ChunkList: chunks}); err != nil {
		return &pb.Status{Code: 1, Message: err.Error()}, err
	}
	m.dumpMetadataAsync()
	return &pb.Status{Code: 0, Message: "deleted"}, nil
}

func (m *MasterServer) RenameFile(ctx context.Context, req *pb.RenameFileRequest) (*pb.Status, error) {
	m.meta.mu.Lock()
	chunks, ok := m.meta.FileMap[req.OldName]
	if ok {
		m.meta.FileMap[req.NewName] = chunks
		delete(m.meta.FileMap, req.OldName)
	}
	m.meta.mu.Unlock()
	if !ok {
		log.Printf("master: rename file %s -> %s failed - source missing", req.OldName, req.NewName)
		return &pb.Status{Code: 1, Message: "file not found"}, errors.New("file not found")
	}
	if err := m.log.Append(opRecord{Op: "rename", File: req.OldName, NewName: req.NewName}); err != nil {
		return &pb.Status{Code: 1, Message: err.Error()}, err
	}
	log.Printf("master: rename file %s -> %s", req.OldName, req.NewName)
	m.dumpMetadataAsync()
	return &pb.Status{Code: 0, Message: "renamed"}, nil
}

func (m *MasterServer) HeartBeat(ctx context.Context, req *pb.HeartBeatRequest) (*pb.HeartBeatResponse, error) {
	chunkVersions := make(map[string]int64, len(req.Chunks))
	var staleHandles []string
	for _, ch := range req.Chunks {
		if ch == nil || ch.Handle == "" {
			continue
		}
		global := m.meta.ChunkVersion(ch.Handle)
		if global > 0 && ch.Version < global {
			staleHandles = append(staleHandles, ch.Handle)
			continue
		}
		chunkVersions[ch.Handle] = ch.Version
		m.meta.UpdateChunkVersion(ch.Handle, ch.Version)
	}
	orphans := m.meta.UpdateReplica(req.Address, chunkVersions)
	for _, handle := range staleHandles {
		if m.meta.MarkReplicaStale(handle, req.Address) {
			log.Printf("master: replica %s reported stale chunk %s (version lag)", req.Address, handle)
			m.enqueueDeleteCommand(handle, []string{req.Address})
		}
	}
	for _, handle := range orphans {
		m.markChunkForGC(handle)
	}
	commands := m.dequeueCommands(req.Address)
	log.Printf("master: heartbeat from %s chunks=%d commands=%d", req.Address, len(req.Chunks), len(commands))
	if len(orphans) > 0 {
		log.Printf("master: heartbeat detected %d orphan chunks: %v", len(orphans), orphans)
	}
	if len(req.Chunks) > 0 || len(commands) > 0 || len(orphans) > 0 {
		m.dumpMetadataAsync()
	}
	m.checkReplicationStatus()
	return &pb.HeartBeatResponse{Commands: commands}, nil
}

func (m *MasterServer) RequestLease(ctx context.Context, req *pb.RequestLeaseRequest) (*pb.RequestLeaseResponse, error) {
	primary, secondaries, version, expiration, err := m.assignLease(req.ChunkHandle, req.Requester)
	if err != nil {
		return &pb.RequestLeaseResponse{Granted: false}, err
	}
	log.Printf("master: lease granted handle=%s primary=%s version=%d expires=%s", req.ChunkHandle, primary, version, expiration.Format(time.RFC3339))
	return &pb.RequestLeaseResponse{
		Granted:             true,
		Primary:             &pb.ChunkLocation{Address: primary},
		Secondaries:         buildLocations(secondaries),
		LeaseExpirationUnix: expiration.Unix(),
		Version:             version,
	}, nil
}

func (m *MasterServer) AllocateChunk(ctx context.Context, req *pb.AllocateChunkRequest) (*pb.AllocateChunkResponse, error) {
	m.meta.mu.Lock()
	chunks, exists := m.meta.FileMap[req.Filename]
	if !exists {
		m.meta.mu.Unlock()
		return nil, errors.New("file does not exist")
	}

	// Allocate new chunk handle
	handle := m.newChunkHandle()
	chunks = append(chunks, handle)
	m.meta.FileMap[req.Filename] = chunks

	// Create chunk metadata
	ci := m.meta.ensureChunkLocked(handle)
	ci.Version = 1

	// Select replica locations from available ChunkServers
	replicas := m.selectReplicasLocked(m.replicationFactor)
	if len(replicas) == 0 {
		m.meta.mu.Unlock()
		return nil, errors.New("no available chunkservers")
	}

	// Assign initial locations (but no lease yet)
	for _, addr := range replicas {
		if m.meta.ChunkLocations[handle] == nil {
			m.meta.ChunkLocations[handle] = make(map[string]struct{})
		}
		m.meta.ChunkLocations[handle][addr] = struct{}{}
	}
	m.meta.mu.Unlock()

	// Log the operation
	if err := m.log.Append(opRecord{Op: "add_chunk", File: req.Filename, Chunk: handle}); err != nil {
		return nil, err
	}
	log.Printf("master: allocated chunk %s for %s replicas=%v", handle, req.Filename, replicas)
	m.dumpMetadataAsync()

	return &pb.AllocateChunkResponse{
		Handle:   handle,
		Replicas: buildLocations(replicas),
	}, nil
}

func (m *MasterServer) assignLease(handle, requester string) (string, []string, int64, time.Time, error) {
	cutoff := time.Now().Add(-replicaLivenessTimeout)
	locations := m.meta.HealthyChunkLocations(handle, cutoff)
	if len(locations) == 0 {
		m.leases.Revoke(handle)
		m.meta.SetLease(handle, "", nil, time.Time{})
		return "", nil, 0, time.Time{}, errors.New("no live replicas available")
	}

	if requester != "" && !contains(locations, requester) {
		requester = ""
	}

	if lease, ok := m.leases.Get(handle); ok {
		preferred := requester
		if preferred == "" && contains(locations, lease.Primary) {
			preferred = lease.Primary
		}
		primary, secondaries := choosePrimary(locations, preferred)
		version := lease.Version
		if primary != lease.Primary {
			version = lease.Version + 1
		}
		refreshed := m.leases.Grant(handle, primary, version, m.leaseDuration)
		m.meta.SetLease(handle, primary, secondaries, refreshed.Expiration)
		m.meta.UpdateChunkVersion(handle, refreshed.Version)
		return primary, secondaries, refreshed.Version, refreshed.Expiration, nil
	}

	primary, secondaries := choosePrimary(locations, requester)
	ci := m.meta.EnsureChunk(handle)
	version := ci.Version + 1
	granted := m.leases.Grant(handle, primary, version, m.leaseDuration)
	m.meta.SetLease(handle, granted.Primary, secondaries, granted.Expiration)
	m.meta.UpdateChunkVersion(handle, granted.Version)
	return granted.Primary, secondaries, granted.Version, granted.Expiration, nil
}

func buildLocations(addrs []string) []*pb.ChunkLocation {
	out := make([]*pb.ChunkLocation, 0, len(addrs))
	for _, a := range addrs {
		out = append(out, &pb.ChunkLocation{Address: a})
	}
	return out
}

func contains(list []string, target string) bool {
	for _, v := range list {
		if v == target {
			return true
		}
	}
	return false
}

func choosePrimary(locations []string, preferred string) (string, []string) {
	if len(locations) == 0 {
		return "", nil
	}
	if preferred != "" && contains(locations, preferred) {
		return preferred, filterOut(locations, preferred)
	}
	idx := rand.Intn(len(locations))
	primary := locations[idx]
	return primary, filterOut(locations, primary)
}

func filterOut(locations []string, target string) []string {
	if len(locations) == 0 {
		return nil
	}
	result := make([]string, 0, len(locations))
	for _, addr := range locations {
		if addr == target {
			continue
		}
		result = append(result, addr)
	}
	return result
}

func (m *MasterServer) selectReplicasLocked(count int) []string {
	// Get all live replicas within the recent heartbeat window
	cutoff := time.Now().Add(-replicaLivenessTimeout)
	var candidates []string
	for addr, state := range m.meta.Replicas {
		if state.LastHeartbeat.After(cutoff) {
			candidates = append(candidates, addr)
		}
	}

	if len(candidates) == 0 {
		return nil
	}

	// Select up to 'count' replicas
	if len(candidates) > count {
		// Shuffle and take first 'count'
		for i := len(candidates) - 1; i > 0; i-- {
			j := rand.Intn(i + 1)
			candidates[i], candidates[j] = candidates[j], candidates[i]
		}
		candidates = candidates[:count]
	}

	return candidates
}

func (m *MasterServer) selectReplicationTarget(handle string, exclude map[string]struct{}) string {
	m.meta.mu.RLock()
	defer m.meta.mu.RUnlock()
	cutoff := time.Now().Add(-replicaLivenessTimeout)
	for addr, state := range m.meta.Replicas {
		if state.LastHeartbeat.Before(cutoff) {
			continue
		}
		if exclude != nil {
			if _, skip := exclude[addr]; skip {
				continue
			}
		}
		if locs, ok := m.meta.ChunkLocations[handle]; ok {
			if _, exists := locs[addr]; exists {
				continue
			}
		}
		if bucket, ok := m.meta.StaleReplicas[handle]; ok {
			if _, stale := bucket[addr]; stale {
				continue
			}
		}
		return addr
	}
	return ""
}

func RunMaster(listenAddr string, logPath string, metaPath string, cfg Config) error {
	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return err
	}
	srv := grpc.NewServer()
	ms, err := NewMasterServer(logPath, metaPath, cfg)
	if err != nil {
		return err
	}
	pb.RegisterMasterServer(srv, ms)
	log.Printf("master listening %s", listenAddr)
	return srv.Serve(lis)
}
