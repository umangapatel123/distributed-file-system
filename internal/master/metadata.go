package master

import (
	"sort"
	"sync"
	"time"
)

type ChunkInfo struct {
	Handle          string
	Version         int64
	Primary         string
	Secondaries     []string
	LeaseExpiration time.Time
}

type ReplicaState struct {
	Address       string
	LastHeartbeat time.Time
	Chunks        map[string]int64 // chunk handle -> version
}

type MasterMetadata struct {
	mu             sync.RWMutex
	FileMap        map[string][]string // filename -> chunk handles (ordered)
	ChunkMap       map[string]*ChunkInfo
	ChunkLocations map[string]map[string]struct{} // handle -> replica addresses
	Replicas       map[string]*ReplicaState
	StaleReplicas  map[string]map[string]struct{}
}

func NewMasterMetadata() *MasterMetadata {
	return &MasterMetadata{
		FileMap:        make(map[string][]string),
		ChunkMap:       make(map[string]*ChunkInfo),
		ChunkLocations: make(map[string]map[string]struct{}),
		Replicas:       make(map[string]*ReplicaState),
		StaleReplicas:  make(map[string]map[string]struct{}),
	}
}

func (m *MasterMetadata) GetFileChunks(name string) []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if v, ok := m.FileMap[name]; ok {
		cp := append([]string(nil), v...)
		return cp
	}
	return nil
}

func (m *MasterMetadata) SetFileChunks(name string, handles []string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.FileMap[name] = append([]string(nil), handles...)
}

func (m *MasterMetadata) EnsureChunk(handle string) *ChunkInfo {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.ensureChunkLocked(handle)
}

func (m *MasterMetadata) UpdateReplica(address string, chunks map[string]int64) []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	st, ok := m.Replicas[address]
	if !ok {
		st = &ReplicaState{Address: address, Chunks: make(map[string]int64)}
		m.Replicas[address] = st
	}
	st.LastHeartbeat = time.Now()
	prev := make([]string, 0, len(st.Chunks))
	for h := range st.Chunks {
		prev = append(prev, h)
	}
	for _, h := range prev {
		delete(st.Chunks, h)
		if locs, ok := m.ChunkLocations[h]; ok {
			delete(locs, address)
			if len(locs) == 0 {
				delete(m.ChunkLocations, h)
			}
		}
		m.clearReplicaStaleLocked(h, address)
	}
	for h, v := range chunks {
		st.Chunks[h] = v
		locs := m.ChunkLocations[h]
		if locs == nil {
			locs = make(map[string]struct{})
			m.ChunkLocations[h] = locs
		}
		locs[address] = struct{}{}
		m.clearReplicaStaleLocked(h, address)
		ci := m.ensureChunkLocked(h)
		if v > ci.Version {
			ci.Version = v
		}
	}
	var orphans []string
	for h := range chunks {
		if !m.chunkReferencedLocked(h) {
			orphans = append(orphans, h)
		}
	}
	return orphans
}

func (m *MasterMetadata) GetChunkLocations(handle string) []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	locs := m.ChunkLocations[handle]
	if locs == nil {
		return nil
	}
	res := make([]string, 0, len(locs))
	for addr := range locs {
		res = append(res, addr)
	}
	sort.Strings(res)
	return res
}

func (m *MasterMetadata) LiveChunkLocations(handle string, cutoff time.Time) []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.healthyChunkLocationsLocked(handle, cutoff)
}

func (m *MasterMetadata) HealthyChunkLocations(handle string, cutoff time.Time) []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.healthyChunkLocationsLocked(handle, cutoff)
}

func (m *MasterMetadata) SetLease(handle string, primary string, secondaries []string, expiration time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ci := m.ensureChunkLocked(handle)
	ci.Primary = primary
	ci.Secondaries = append([]string(nil), secondaries...)
	ci.LeaseExpiration = expiration
}

func (m *MasterMetadata) GetLease(handle string) (primary string, secondaries []string, expiration time.Time, ok bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	ci, exists := m.ChunkMap[handle]
	if !exists {
		return "", nil, time.Time{}, false
	}
	return ci.Primary, append([]string(nil), ci.Secondaries...), ci.LeaseExpiration, true
}

func (m *MasterMetadata) UpdateChunkVersion(handle string, version int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ci := m.ensureChunkLocked(handle)
	if version > ci.Version {
		ci.Version = version
	}
}

func (m *MasterMetadata) ensureChunkLocked(handle string) *ChunkInfo {
	if ci, ok := m.ChunkMap[handle]; ok {
		return ci
	}
	ci := &ChunkInfo{Handle: handle, Version: 1}
	m.ChunkMap[handle] = ci
	return ci
}

func (m *MasterMetadata) RemoveFile(name string) ([]string, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	chunks, ok := m.FileMap[name]
	if !ok {
		return nil, false
	}
	delete(m.FileMap, name)
	return append([]string(nil), chunks...), true
}

func (m *MasterMetadata) RemoveChunk(handle string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.ChunkMap, handle)
	delete(m.ChunkLocations, handle)
	delete(m.StaleReplicas, handle)
	for _, replica := range m.Replicas {
		delete(replica.Chunks, handle)
	}
}

func (m *MasterMetadata) chunkReferencedLocked(handle string) bool {
	for _, chunks := range m.FileMap {
		for _, h := range chunks {
			if h == handle {
				return true
			}
		}
	}
	return false
}

func (m *MasterMetadata) ListChunkHandles() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]string, 0, len(m.ChunkMap))
	for handle := range m.ChunkMap {
		result = append(result, handle)
	}
	return result
}

func (m *MasterMetadata) ChunkVersion(handle string) int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if ci, ok := m.ChunkMap[handle]; ok {
		return ci.Version
	}
	return 0
}

func (m *MasterMetadata) MarkReplicaStale(handle, addr string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.markReplicaStaleLocked(handle, addr)
}

func (m *MasterMetadata) ClearReplicaStale(handle, addr string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.clearReplicaStaleLocked(handle, addr)
}

func (m *MasterMetadata) ReplicaHealth(handle string, cutoff time.Time) (healthy []string, stale []string) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	healthy = m.healthyChunkLocationsLocked(handle, cutoff)
	if bucket, ok := m.StaleReplicas[handle]; ok {
		for addr := range bucket {
			if st, ok := m.Replicas[addr]; ok && st.LastHeartbeat.After(cutoff) {
				stale = append(stale, addr)
			}
		}
		sort.Strings(stale)
	}
	return
}

func (m *MasterMetadata) healthyChunkLocationsLocked(handle string, cutoff time.Time) []string {
	locs := m.ChunkLocations[handle]
	if locs == nil {
		return nil
	}
	res := make([]string, 0, len(locs))
	for addr := range locs {
		if m.isReplicaStaleLocked(handle, addr) {
			continue
		}
		if st, ok := m.Replicas[addr]; ok && st.LastHeartbeat.After(cutoff) {
			res = append(res, addr)
		}
	}
	sort.Strings(res)
	return res
}

func (m *MasterMetadata) markReplicaStaleLocked(handle, addr string) bool {
	bucket := m.StaleReplicas[handle]
	if bucket == nil {
		bucket = make(map[string]struct{})
		m.StaleReplicas[handle] = bucket
	}
	if _, exists := bucket[addr]; exists {
		return false
	}
	bucket[addr] = struct{}{}
	if locs, ok := m.ChunkLocations[handle]; ok {
		delete(locs, addr)
		if len(locs) == 0 {
			delete(m.ChunkLocations, handle)
		}
	}
	if replica, ok := m.Replicas[addr]; ok {
		delete(replica.Chunks, handle)
	}
	return true
}

func (m *MasterMetadata) clearReplicaStaleLocked(handle, addr string) {
	if bucket, ok := m.StaleReplicas[handle]; ok {
		delete(bucket, addr)
		if len(bucket) == 0 {
			delete(m.StaleReplicas, handle)
		}
	}
}

func (m *MasterMetadata) isReplicaStaleLocked(handle, addr string) bool {
	if bucket, ok := m.StaleReplicas[handle]; ok {
		_, exists := bucket[addr]
		return exists
	}
	return false
}
