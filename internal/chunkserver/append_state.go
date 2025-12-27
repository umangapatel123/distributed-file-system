package chunkserver

import (
	"sync"
	"time"
)

const appendHistoryLimit = 128

type appendStatus int

const (
	appendStatusPreparing appendStatus = iota
	appendStatusPrepared
	appendStatusCommitted
)

type appendRecordState struct {
	offset           int64
	length           int64
	originalSize     int64
	status           appendStatus
	committedVersion int64
	lastUpdated      time.Time
}

func (cs *ChunkServer) getChunkLock(handle string) *sync.Mutex {
	cs.chunkLockMu.Lock()
	defer cs.chunkLockMu.Unlock()
	if lock, ok := cs.chunkLocks[handle]; ok {
		return lock
	}
	lock := &sync.Mutex{}
	cs.chunkLocks[handle] = lock
	return lock
}

func (cs *ChunkServer) lookupAppendState(handle, id string) (*appendRecordState, bool) {
	cs.appendStateMu.Lock()
	defer cs.appendStateMu.Unlock()
	states, ok := cs.appendStates[handle]
	if !ok {
		return nil, false
	}
	st, ok := states[id]
	if !ok {
		return nil, false
	}
	return st, true
}

func (cs *ChunkServer) saveAppendState(handle, id string, state *appendRecordState) {
	cs.appendStateMu.Lock()
	defer cs.appendStateMu.Unlock()
	bucket := cs.appendStates[handle]
	if bucket == nil {
		bucket = make(map[string]*appendRecordState)
		cs.appendStates[handle] = bucket
	}
	state.lastUpdated = time.Now()
	bucket[id] = state
	pruneAppendStates(bucket)
	cs.markStateDirty()
}

func (cs *ChunkServer) updateAppendStateStatus(handle, id string, status appendStatus, version int64) {
	cs.appendStateMu.Lock()
	defer cs.appendStateMu.Unlock()
	bucket, ok := cs.appendStates[handle]
	if !ok {
		return
	}
	st, ok := bucket[id]
	if !ok {
		return
	}
	st.status = status
	if status == appendStatusCommitted {
		st.committedVersion = version
	}
	st.lastUpdated = time.Now()
	cs.markStateDirty()
}

func (cs *ChunkServer) deleteAppendState(handle, id string) {
	cs.appendStateMu.Lock()
	defer cs.appendStateMu.Unlock()
	bucket, ok := cs.appendStates[handle]
	if !ok {
		return
	}
	delete(bucket, id)
	if len(bucket) == 0 {
		delete(cs.appendStates, handle)
	}
	cs.markStateDirty()
}

func pruneAppendStates(bucket map[string]*appendRecordState) {
	if len(bucket) <= appendHistoryLimit {
		return
	}
	var oldestKey string
	var oldestTime time.Time
	for key, state := range bucket {
		if state.status != appendStatusCommitted {
			continue
		}
		if oldestKey == "" || state.lastUpdated.Before(oldestTime) {
			oldestKey = key
			oldestTime = state.lastUpdated
		}
	}
	if oldestKey != "" {
		delete(bucket, oldestKey)
	}
}

func (cs *ChunkServer) touchAppendState(handle, id string) {
	cs.appendStateMu.Lock()
	defer cs.appendStateMu.Unlock()
	if bucket, ok := cs.appendStates[handle]; ok {
		if st, ok := bucket[id]; ok {
			st.lastUpdated = time.Now()
			cs.markStateDirty()
		}
	}
}
