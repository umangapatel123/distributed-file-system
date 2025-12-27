package chunkserver

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"
)

type persistedAppendState struct {
	Offset           int64        `json:"offset"`
	Length           int64        `json:"length"`
	OriginalSize     int64        `json:"original_size"`
	Status           appendStatus `json:"status"`
	CommittedVersion int64        `json:"committed_version"`
	LastUpdated      time.Time    `json:"last_updated"`
}

type persistedChunkServerState struct {
	ChunkSizes    map[string]int64                           `json:"chunk_sizes"`
	ChunkVersions map[string]int64                           `json:"chunk_versions"`
	AppendStates  map[string]map[string]persistedAppendState `json:"append_states"`
}

const (
	stateFileName        = "chunkserver_state.json"
	defaultStateSyncFreq = 5 * time.Second
)

func (cs *ChunkServer) initStatePersistence() {
	if cs.dataDir == "" {
		return
	}
	cs.statePath = filepath.Join(cs.dataDir, stateFileName)
	if cs.stateSyncInterval <= 0 {
		cs.stateSyncInterval = defaultStateSyncFreq
	}
	cs.stateStop = make(chan struct{})
	cs.stateVersion = 1
	cs.statePersistedVersion = 0
	cs.loadPersistentState()
	cs.statePersistedVersion = cs.stateVersion
	cs.stateWg.Add(1)
	go cs.stateSyncLoop()
}

func (cs *ChunkServer) stateSyncLoop() {
	defer cs.stateWg.Done()
	ticker := time.NewTicker(cs.stateSyncInterval)
	defer ticker.Stop()
	for {
		select {
		case <-cs.stateStop:
			cs.flushStateIfDirty()
			return
		case <-ticker.C:
			cs.flushStateIfDirty()
		}
	}
}

func (cs *ChunkServer) markStateDirty() {
	atomic.AddUint64(&cs.stateVersion, 1)
}

func (cs *ChunkServer) flushStateIfDirty() {
	if cs.dataDir == "" || cs.statePath == "" {
		return
	}
	current := atomic.LoadUint64(&cs.stateVersion)
	persisted := atomic.LoadUint64(&cs.statePersistedVersion)
	if current == persisted {
		return
	}
	if err := cs.persistState(); err != nil {
		log.Printf("chunkserver %s: failed to persist state: %v", cs.addr, err)
		return
	}
	atomic.StoreUint64(&cs.statePersistedVersion, current)
}

func (cs *ChunkServer) persistState() error {
	snapshot := persistedChunkServerState{
		ChunkSizes:    make(map[string]int64),
		ChunkVersions: make(map[string]int64),
		AppendStates:  make(map[string]map[string]persistedAppendState),
	}

	cs.mu.Lock()
	for handle, size := range cs.chunkSizes {
		snapshot.ChunkSizes[handle] = size
	}
	for handle, ver := range cs.chunkVersion {
		snapshot.ChunkVersions[handle] = ver
	}
	cs.mu.Unlock()

	cs.appendStateMu.Lock()
	for handle, bucket := range cs.appendStates {
		if len(bucket) == 0 {
			continue
		}
		dst := make(map[string]persistedAppendState, len(bucket))
		for id, state := range bucket {
			dst[id] = persistedAppendState{
				Offset:           state.offset,
				Length:           state.length,
				OriginalSize:     state.originalSize,
				Status:           state.status,
				CommittedVersion: state.committedVersion,
				LastUpdated:      state.lastUpdated,
			}
		}
		snapshot.AppendStates[handle] = dst
	}
	cs.appendStateMu.Unlock()

	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return err
	}
	tmp := cs.statePath + ".tmp"
	if err := ensureDir(filepath.Dir(cs.statePath)); err != nil {
		return err
	}
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return err
	}
	return os.Rename(tmp, cs.statePath)
}

func (cs *ChunkServer) loadPersistentState() {
	if cs.statePath == "" {
		return
	}
	data, err := os.ReadFile(cs.statePath)
	if err != nil {
		if os.IsNotExist(err) {
			return
		}
		log.Printf("chunkserver %s: failed to read state file: %v", cs.addr, err)
		return
	}
	var snapshot persistedChunkServerState
	if err := json.Unmarshal(data, &snapshot); err != nil {
		log.Printf("chunkserver %s: failed to parse state file: %v", cs.addr, err)
		return
	}

	cs.mu.Lock()
	for handle, size := range snapshot.ChunkSizes {
		cs.chunkSizes[handle] = size
	}
	for handle, ver := range snapshot.ChunkVersions {
		if cur, ok := cs.chunkVersion[handle]; !ok || ver > cur {
			cs.chunkVersion[handle] = ver
		}
	}
	cs.mu.Unlock()

	cs.appendStateMu.Lock()
	for handle, bucket := range snapshot.AppendStates {
		if cs.appendStates[handle] == nil {
			cs.appendStates[handle] = make(map[string]*appendRecordState)
		}
		for id, state := range bucket {
			st := &appendRecordState{
				offset:           state.Offset,
				length:           state.Length,
				originalSize:     state.OriginalSize,
				status:           state.Status,
				committedVersion: state.CommittedVersion,
				lastUpdated:      state.LastUpdated,
			}
			cs.appendStates[handle][id] = st
		}
	}
	cs.appendStateMu.Unlock()
}
