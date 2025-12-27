package chunkserver

import (
	"log"
	"sync"
	"time"
)

const (
	preparedTransactionTimeout = 10 * time.Second
	recoveryCheckInterval      = 2 * time.Second
)

// PreparedTransactionMonitor monitors and recovers stuck prepared transactions
type PreparedTransactionMonitor struct {
	cs       *ChunkServer
	stopChan chan struct{}
	wg       sync.WaitGroup
}

func NewPreparedTransactionMonitor(cs *ChunkServer) *PreparedTransactionMonitor {
	return &PreparedTransactionMonitor{
		cs:       cs,
		stopChan: make(chan struct{}),
	}
}

func (ptm *PreparedTransactionMonitor) Start() {
	ptm.wg.Add(1)
	go ptm.monitorLoop()
}

func (ptm *PreparedTransactionMonitor) Stop() {
	close(ptm.stopChan)
	ptm.wg.Wait()
}

func (ptm *PreparedTransactionMonitor) monitorLoop() {
	defer ptm.wg.Done()

	ticker := time.NewTicker(recoveryCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ptm.stopChan:
			return
		case <-ticker.C:
			ptm.checkPreparedTransactions()
		}
	}
}

func (ptm *PreparedTransactionMonitor) checkPreparedTransactions() {
	ptm.cs.appendStateMu.Lock()
	
	stalePreparedTransactions := make(map[string]map[string]*appendRecordState)
	
	for handle, bucket := range ptm.cs.appendStates {
		for id, state := range bucket {
			if state.status == appendStatusPrepared {
				age := time.Since(state.lastUpdated)
				if age > preparedTransactionTimeout {
					if stalePreparedTransactions[handle] == nil {
						stalePreparedTransactions[handle] = make(map[string]*appendRecordState)
					}
					stalePreparedTransactions[handle][id] = state
					log.Printf("chunkserver %s: found stale prepared transaction handle=%s id=%s age=%v",
						ptm.cs.addr, handle, id, age)
				}
			}
		}
	}
	
	ptm.cs.appendStateMu.Unlock()

	// Attempt recovery for stale transactions
	for handle, transactions := range stalePreparedTransactions {
		for id, state := range transactions {
			ptm.recoverPreparedTransaction(handle, id, state)
		}
	}
}

func (ptm *PreparedTransactionMonitor) recoverPreparedTransaction(handle, id string, state *appendRecordState) {
	lock := ptm.cs.getChunkLock(handle)
	lock.Lock()
	defer lock.Unlock()

	// Double-check state is still prepared
	currentState, ok := ptm.cs.lookupAppendState(handle, id)
	if !ok || currentState.status != appendStatusPrepared {
		return
	}

	log.Printf("chunkserver %s: attempting to abort stale prepared transaction handle=%s id=%s",
		ptm.cs.addr, handle, id)

	// Abort the stale prepared transaction
	if err := ptm.cs.abortLocalAppendLocked(handle, id, currentState); err != nil {
		log.Printf("chunkserver %s: failed to abort stale transaction handle=%s id=%s: %v",
			ptm.cs.addr, handle, id, err)
		return
	}

	ptm.cs.deleteAppendState(handle, id)
	log.Printf("chunkserver %s: aborted stale prepared transaction handle=%s id=%s",
		ptm.cs.addr, handle, id)
}
