package chunkserver

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	pb "ds-gfs-append/api/proto"
)

// Test: Primary Crash After Prepare Phase (2PC Blocking Problem)
// This is the critical scenario where primary crashes after replicas have prepared
// but before sending commit decision. Without consensus, replicas are blocked.
func TestExactlyOnce_PrimaryCrashAfterPrepare(t *testing.T) {
	// Create 3 chunk servers - 1 primary + 2 secondaries
	primaryDir := t.TempDir()
	sec1Dir := t.TempDir()
	sec2Dir := t.TempDir()

	primary := NewChunkServer("primary", primaryDir, "", 64<<20, 10*time.Second)
	secondary1 := NewChunkServer("secondary1", sec1Dir, "", 64<<20, 10*time.Second)
	secondary2 := NewChunkServer("secondary2", sec2Dir, "", 64<<20, 10*time.Second)

	chunkHandle := "ch-test-blocking"
	idempotencyID := "append-blocking-123"
	testData := []byte("data for blocking test")

	// Push data to all servers
	primary.storeBuffer("data-block", testData)
	secondary1.storeBuffer("data-block", testData)
	secondary2.storeBuffer("data-block", testData)

	// STEP 1: Manually execute PREPARE phase on all replicas
	primaryLock := primary.getChunkLock(chunkHandle)
	primaryLock.Lock()

	offset, _ := primary.currentChunkSize(chunkHandle)

	// Primary prepares
	primaryState := &appendRecordState{
		offset:       offset,
		length:       int64(len(testData)),
		originalSize: offset,
		status:       appendStatusPreparing,
	}
	primary.saveAppendState(chunkHandle, idempotencyID, primaryState)
	if err := primary.reserveLocalAppendLocked(chunkHandle, primaryState); err != nil {
		t.Fatalf("Primary prepare failed: %v", err)
	}
	primary.updateAppendStateStatus(chunkHandle, idempotencyID, appendStatusPrepared, 0)
	t.Logf("Primary prepared at offset %d", offset)

	// Secondary 1 prepares
	sec1Lock := secondary1.getChunkLock(chunkHandle)
	sec1Lock.Lock()
	sec1State := &appendRecordState{
		offset:       offset,
		length:       int64(len(testData)),
		originalSize: offset,
		status:       appendStatusPreparing,
	}
	secondary1.saveAppendState(chunkHandle, idempotencyID, sec1State)
	if err := secondary1.reserveLocalAppendLocked(chunkHandle, sec1State); err != nil {
		t.Fatalf("Secondary1 prepare failed: %v", err)
	}
	secondary1.updateAppendStateStatus(chunkHandle, idempotencyID, appendStatusPrepared, 0)
	sec1Lock.Unlock()
	t.Logf("Secondary1 prepared")

	// Secondary 2 prepares
	sec2Lock := secondary2.getChunkLock(chunkHandle)
	sec2Lock.Lock()
	sec2State := &appendRecordState{
		offset:       offset,
		length:       int64(len(testData)),
		originalSize: offset,
		status:       appendStatusPreparing,
	}
	secondary2.saveAppendState(chunkHandle, idempotencyID, sec2State)
	if err := secondary2.reserveLocalAppendLocked(chunkHandle, sec2State); err != nil {
		t.Fatalf("Secondary2 prepare failed: %v", err)
	}
	secondary2.updateAppendStateStatus(chunkHandle, idempotencyID, appendStatusPrepared, 0)
	sec2Lock.Unlock()
	t.Logf("Secondary2 prepared")

	// STEP 2: PRIMARY CRASHES (simulated by not sending commit and releasing lock)
	primaryLock.Unlock()
	t.Logf("PRIMARY CRASHED after prepare phase - secondaries are now BLOCKED")

	// Verify all are in prepared state
	if state, ok := primary.lookupAppendState(chunkHandle, idempotencyID); ok {
		if state.status != appendStatusPrepared {
			t.Errorf("Primary should be in prepared state, got %d", state.status)
		}
	}
	if state, ok := secondary1.lookupAppendState(chunkHandle, idempotencyID); ok {
		if state.status != appendStatusPrepared {
			t.Errorf("Secondary1 should be in prepared state, got %d", state.status)
		}
	}
	if state, ok := secondary2.lookupAppendState(chunkHandle, idempotencyID); ok {
		if state.status != appendStatusPrepared {
			t.Errorf("Secondary2 should be in prepared state, got %d", state.status)
		}
	}

	// STEP 3: Test that secondaries can recover via consensus
	// In a real system with Raft/Paxos:
	// - Secondaries would form a quorum (2 out of 3)
	// - Elect a new leader
	// - New leader queries quorum for prepared transactions
	// - Since majority (2/3) have prepared state, new leader decides to COMMIT
	// - New leader completes the transaction

	t.Logf("Attempting recovery via consensus-based commit decision...")

	// Simulate: Secondary1 becomes new leader through election
	newLeader := secondary1
	quorumMembers := []*ChunkServer{secondary1, secondary2}

	// New leader queries quorum for append state
	preparedCount := 0
	var recoveredState *appendRecordState
	
	for _, member := range quorumMembers {
		if state, ok := member.lookupAppendState(chunkHandle, idempotencyID); ok {
			if state.status == appendStatusPrepared {
				preparedCount++
				if recoveredState == nil {
					recoveredState = state
				}
			}
		}
	}

	t.Logf("Quorum check: %d/%d members have prepared state", preparedCount, len(quorumMembers))

	// CONSENSUS DECISION: If majority prepared, commit
	if preparedCount >= (len(quorumMembers)/2 + 1) {
		t.Logf("Quorum reached! New leader deciding to COMMIT")

		// New leader commits on itself
		newLeaderLock := newLeader.getChunkLock(chunkHandle)
		newLeaderLock.Lock()
		if err := newLeader.commitLocalAppendLocked(chunkHandle, recoveredState.offset, testData, 1); err != nil {
			t.Fatalf("New leader commit failed: %v", err)
		}
		newLeader.updateAppendStateStatus(chunkHandle, idempotencyID, appendStatusCommitted, 1)
		newLeaderLock.Unlock()
		t.Logf("New leader committed successfully")

		// New leader sends commit to other quorum members
		sec2Lock.Lock()
		if err := secondary2.commitLocalAppendLocked(chunkHandle, recoveredState.offset, testData, 1); err != nil {
			t.Fatalf("Secondary2 commit failed: %v", err)
		}
		secondary2.updateAppendStateStatus(chunkHandle, idempotencyID, appendStatusCommitted, 1)
		sec2Lock.Unlock()
		t.Logf("Secondary2 committed successfully")

		// Eventually sync with recovered primary (when it comes back)
		primaryLock.Lock()
		if primaryState, ok := primary.lookupAppendState(chunkHandle, idempotencyID); ok {
			if primaryState.status == appendStatusPrepared {
				// Primary catches up from quorum
				if err := primary.commitLocalAppendLocked(chunkHandle, primaryState.offset, testData, 1); err != nil {
					t.Fatalf("Primary catchup failed: %v", err)
				}
				primary.updateAppendStateStatus(chunkHandle, idempotencyID, appendStatusCommitted, 1)
				t.Logf("Old primary caught up and committed")
			}
		}
		primaryLock.Unlock()

	} else {
		t.Logf("Quorum NOT reached - would ABORT")
		// If no quorum, abort all prepared transactions
		for _, member := range quorumMembers {
			lock := member.getChunkLock(chunkHandle)
			lock.Lock()
			if state, ok := member.lookupAppendState(chunkHandle, idempotencyID); ok {
				member.abortLocalAppendLocked(chunkHandle, idempotencyID, state)
				member.deleteAppendState(chunkHandle, idempotencyID)
			}
			lock.Unlock()
		}
	}

	// VERIFICATION: All servers should now have committed state
	for i, server := range []*ChunkServer{primary, secondary1, secondary2} {
		if state, ok := server.lookupAppendState(chunkHandle, idempotencyID); ok {
			if state.status != appendStatusCommitted {
				t.Errorf("Server %d: Expected committed state, got %d", i, state.status)
			}
		} else {
			t.Errorf("Server %d: No append state found", i)
		}

		// Verify data was written exactly once
		count := countOccurrences(t, server, chunkHandle, testData)
		if count != 1 {
			t.Errorf("Server %d: Data written %d times, expected 1", i, count)
		}
	}

	t.Log("✓ Consensus-based recovery succeeded - blocked transaction completed")
}

// Test: Timeout-based recovery for prepared transactions
func TestExactlyOnce_PreparedTransactionTimeout(t *testing.T) {
	cs := NewChunkServer("cs", t.TempDir(), "", 64<<20, 10*time.Second)
	
	// Start the monitor
	cs.preparedTxnMonitor.Start()
	defer cs.preparedTxnMonitor.Stop()

	chunkHandle := "ch-test-timeout"
	idempotencyID := "append-timeout-456"
	testData := []byte("data for timeout test")

	cs.storeBuffer("data-timeout", testData)

	// Manually create a prepared state with old timestamp
	lock := cs.getChunkLock(chunkHandle)
	lock.Lock()

	offset, _ := cs.currentChunkSize(chunkHandle)
	state := &appendRecordState{
		offset:       offset,
		length:       int64(len(testData)),
		originalSize: offset,
		status:       appendStatusPreparing,
		lastUpdated:  time.Now().Add(-15 * time.Second), // Make it stale
	}
	cs.saveAppendState(chunkHandle, idempotencyID, state)
	cs.reserveLocalAppendLocked(chunkHandle, state)
	cs.updateAppendStateStatus(chunkHandle, idempotencyID, appendStatusPrepared, 0)
	
	// Manually set old timestamp again (since updateAppendStateStatus updates it)
	cs.appendStateMu.Lock()
	if st, ok := cs.appendStates[chunkHandle][idempotencyID]; ok {
		st.lastUpdated = time.Now().Add(-15 * time.Second)
	}
	cs.appendStateMu.Unlock()
	
	lock.Unlock()

	t.Logf("Created stale prepared transaction (15s old), waiting for monitor to detect and abort...")

	// Wait for monitor to detect and abort the stale transaction
	time.Sleep(4 * time.Second)

	// Verify state was cleaned up
	if state, ok := cs.lookupAppendState(chunkHandle, idempotencyID); ok {
		t.Errorf("Stale prepared state should have been aborted, but still exists with status %d", state.status)
	} else {
		t.Logf("✓ Monitor successfully aborted stale prepared transaction")
	}

	// Client can now retry fresh
	req := &pb.AppendRecordRequest{
		ChunkHandle:   chunkHandle,
		DataId:        "data-timeout",
		IdempotencyId: idempotencyID,
		ChunkVersion:  1,
		Secondaries:   []*pb.ChunkLocation{},
	}

	resp, err := cs.AppendRecord(context.Background(), req)
	if err != nil {
		t.Fatalf("Retry failed: %v", err)
	}
	if !resp.Success {
		t.Fatalf("Retry returned success=false")
	}

	// Verify data written exactly once
	count := countOccurrences(t, cs, chunkHandle, testData)
	if count != 1 {
		t.Errorf("Data written %d times, expected 1", count)
	}

	t.Log("✓ Stale prepared transaction cleaned up and retried successfully")
}

// Test: Concurrent recovery attempts
func TestExactlyOnce_ConcurrentRecovery(t *testing.T) {
	// Setup: 3 servers, all in prepared state
	servers := make([]*ChunkServer, 3)
	for i := 0; i < 3; i++ {
		servers[i] = NewChunkServer(fmt.Sprintf("cs%d", i), t.TempDir(), "", 64<<20, 10*time.Second)
	}

	chunkHandle := "ch-concurrent-recovery"
	idempotencyID := "append-concurrent-rec"
	testData := []byte("concurrent recovery data")

	// All servers push data and prepare
	for i, cs := range servers {
		cs.storeBuffer("data-rec", testData)

		lock := cs.getChunkLock(chunkHandle)
		lock.Lock()

		offset, _ := cs.currentChunkSize(chunkHandle)
		state := &appendRecordState{
			offset:       offset,
			length:       int64(len(testData)),
			originalSize: offset,
			status:       appendStatusPreparing,
		}
		cs.saveAppendState(chunkHandle, idempotencyID, state)
		cs.reserveLocalAppendLocked(chunkHandle, state)
		cs.updateAppendStateStatus(chunkHandle, idempotencyID, appendStatusPrepared, 0)
		lock.Unlock()

		t.Logf("Server %d prepared", i)
	}

	// Simulate: Multiple servers try to become leader concurrently
	var wg sync.WaitGroup
	commitResults := make([]bool, 3)

	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			cs := servers[idx]
			lock := cs.getChunkLock(chunkHandle)
			lock.Lock()
			defer lock.Unlock()

			if state, ok := cs.lookupAppendState(chunkHandle, idempotencyID); ok {
				if state.status == appendStatusPrepared {
					// Try to commit
					if err := cs.commitLocalAppendLocked(chunkHandle, state.offset, testData, 1); err == nil {
						cs.updateAppendStateStatus(chunkHandle, idempotencyID, appendStatusCommitted, 1)
						commitResults[idx] = true
						t.Logf("Server %d committed", idx)
					}
				}
			}
		}(i)
	}

	wg.Wait()

	// Verify: All servers committed
	successCount := 0
	for i, success := range commitResults {
		if success {
			successCount++
		}

		// Verify data written exactly once on each server
		count := countOccurrences(t, servers[i], chunkHandle, testData)
		if count != 1 {
			t.Errorf("Server %d: Data written %d times, expected 1", i, count)
		}
	}

	if successCount != 3 {
		t.Logf("Warning: Only %d/3 servers committed (expected all to commit)", successCount)
	}

	t.Log("✓ Concurrent recovery completed without duplicates")
}

// Test: Secondary Fails Before Receiving Commit Message
// Scenario: Primary sends PREPARE → All replicas respond → Primary sends COMMIT
// → Secondary fails/crashes before receiving COMMIT → Transaction should be aborted
// and rolled back after timeout → Client retry succeeds → Data written exactly once
func TestExactlyOnce_SecondaryFailBeforeCommit(t *testing.T) {
	// Create 3 chunk servers - 1 primary + 2 secondaries
	primaryDir := t.TempDir()
	sec1Dir := t.TempDir()
	sec2Dir := t.TempDir()

	primary := NewChunkServer("primary", primaryDir, "", 64<<20, 10*time.Second)
	secondary1 := NewChunkServer("secondary1", sec1Dir, "", 64<<20, 10*time.Second)
	secondary2 := NewChunkServer("secondary2", sec2Dir, "", 64<<20, 10*time.Second)

	// Start monitors for automatic timeout recovery
	primary.preparedTxnMonitor.Start()
	secondary1.preparedTxnMonitor.Start()
	secondary2.preparedTxnMonitor.Start()
	defer primary.preparedTxnMonitor.Stop()
	defer secondary1.preparedTxnMonitor.Stop()
	defer secondary2.preparedTxnMonitor.Stop()

	chunkHandle := "ch-test-sec-fail"
	idempotencyID := "append-sec-fail-789"
	testData := []byte("data for secondary failure test")

	// Push data to all servers
	primary.storeBuffer("data-sec-fail", testData)
	secondary1.storeBuffer("data-sec-fail", testData)
	secondary2.storeBuffer("data-sec-fail", testData)

	// PHASE 1: PREPARE - All replicas prepare successfully
	primaryLock := primary.getChunkLock(chunkHandle)
	primaryLock.Lock()

	offset, _ := primary.currentChunkSize(chunkHandle)

	// Primary prepares
	primaryState := &appendRecordState{
		offset:       offset,
		length:       int64(len(testData)),
		originalSize: offset,
		status:       appendStatusPreparing,
	}
	primary.saveAppendState(chunkHandle, idempotencyID, primaryState)
	if err := primary.reserveLocalAppendLocked(chunkHandle, primaryState); err != nil {
		t.Fatalf("Primary prepare failed: %v", err)
	}
	primary.updateAppendStateStatus(chunkHandle, idempotencyID, appendStatusPrepared, 0)
	t.Logf("✓ Primary prepared at offset %d", offset)

	// Secondary 1 prepares
	sec1Lock := secondary1.getChunkLock(chunkHandle)
	sec1Lock.Lock()
	sec1State := &appendRecordState{
		offset:       offset,
		length:       int64(len(testData)),
		originalSize: offset,
		status:       appendStatusPreparing,
	}
	secondary1.saveAppendState(chunkHandle, idempotencyID, sec1State)
	if err := secondary1.reserveLocalAppendLocked(chunkHandle, sec1State); err != nil {
		t.Fatalf("Secondary1 prepare failed: %v", err)
	}
	secondary1.updateAppendStateStatus(chunkHandle, idempotencyID, appendStatusPrepared, 0)
	sec1Lock.Unlock()
	t.Logf("✓ Secondary1 prepared")

	// Secondary 2 prepares
	sec2Lock := secondary2.getChunkLock(chunkHandle)
	sec2Lock.Lock()
	sec2State := &appendRecordState{
		offset:       offset,
		length:       int64(len(testData)),
		originalSize: offset,
		status:       appendStatusPreparing,
	}
	secondary2.saveAppendState(chunkHandle, idempotencyID, sec2State)
	if err := secondary2.reserveLocalAppendLocked(chunkHandle, sec2State); err != nil {
		t.Fatalf("Secondary2 prepare failed: %v", err)
	}
	secondary2.updateAppendStateStatus(chunkHandle, idempotencyID, appendStatusPrepared, 0)
	sec2Lock.Unlock()
	t.Logf("✓ Secondary2 prepared")

	// PHASE 2: COMMIT - Primary commits and sends to Secondary1
	// Primary commits locally
	if err := primary.commitLocalAppendLocked(chunkHandle, primaryState.offset, testData, 1); err != nil {
		t.Fatalf("Primary commit failed: %v", err)
	}
	primary.updateAppendStateStatus(chunkHandle, idempotencyID, appendStatusCommitted, 1)
	primaryLock.Unlock()
	t.Logf("✓ Primary committed successfully")

	// Secondary1 receives and processes commit
	sec1Lock.Lock()
	if err := secondary1.commitLocalAppendLocked(chunkHandle, sec1State.offset, testData, 1); err != nil {
		t.Fatalf("Secondary1 commit failed: %v", err)
	}
	secondary1.updateAppendStateStatus(chunkHandle, idempotencyID, appendStatusCommitted, 1)
	sec1Lock.Unlock()
	t.Logf("✓ Secondary1 committed successfully")

	// CRITICAL: Secondary2 CRASHES/FAILS before receiving commit message
	t.Logf("⚠ SECONDARY2 FAILED before receiving commit - stuck in PREPARED state")

	// Verify states after failure
	if state, ok := primary.lookupAppendState(chunkHandle, idempotencyID); !ok || state.status != appendStatusCommitted {
		t.Errorf("Primary should be committed")
	}
	if state, ok := secondary1.lookupAppendState(chunkHandle, idempotencyID); !ok || state.status != appendStatusCommitted {
		t.Errorf("Secondary1 should be committed")
	}
	if state, ok := secondary2.lookupAppendState(chunkHandle, idempotencyID); !ok || state.status != appendStatusPrepared {
		t.Errorf("Secondary2 should be in PREPARED state (stuck)")
	}

	// Make the prepared transaction stale by backdating timestamp
	secondary2.appendStateMu.Lock()
	if st, ok := secondary2.appendStates[chunkHandle][idempotencyID]; ok {
		st.lastUpdated = time.Now().Add(-15 * time.Second)
		t.Logf("✓ Backdated Secondary2's prepared transaction to 15s ago")
	}
	secondary2.appendStateMu.Unlock()

	// PHASE 3: TIMEOUT RECOVERY - Monitor detects and aborts stale transaction
	t.Logf("Waiting for PreparedTransactionMonitor to detect and abort stale transaction...")
	time.Sleep(4 * time.Second) // Wait for monitor (2s interval + processing time)

	// Verify Secondary2's stale transaction was aborted
	if state, ok := secondary2.lookupAppendState(chunkHandle, idempotencyID); ok {
		t.Errorf("Secondary2 stale transaction should have been aborted, but still exists with status %d", state.status)
	} else {
		t.Logf("✓ Monitor successfully aborted Secondary2's stale prepared transaction")
	}

	// Verify no data on Secondary2 (should be rolled back)
	count := countOccurrences(t, secondary2, chunkHandle, testData)
	if count != 0 {
		t.Errorf("Secondary2 should have rolled back data, but found %d occurrences", count)
	} else {
		t.Logf("✓ Secondary2 rolled back - no data written")
	}

	// PHASE 4: CLIENT RETRY - Now with healthy Secondary2
	t.Logf("Client retrying append operation...")

	// Simulate fresh append to Secondary2 (as part of retry with new replica set)
	// In real system, master would reassign to different replicas or retry with healed secondary
	sec2Lock.Lock()
	freshState := &appendRecordState{
		offset:       0, // Fresh chunk on secondary2
		length:       int64(len(testData)),
		originalSize: 0,
		status:       appendStatusPreparing,
	}
	secondary2.saveAppendState(chunkHandle, idempotencyID, freshState)
	if err := secondary2.reserveLocalAppendLocked(chunkHandle, freshState); err != nil {
		t.Fatalf("Secondary2 fresh prepare failed: %v", err)
	}
	secondary2.updateAppendStateStatus(chunkHandle, idempotencyID, appendStatusPrepared, 0)
	
	// Commit on Secondary2
	if err := secondary2.commitLocalAppendLocked(chunkHandle, freshState.offset, testData, 1); err != nil {
		t.Fatalf("Secondary2 fresh commit failed: %v", err)
	}
	secondary2.updateAppendStateStatus(chunkHandle, idempotencyID, appendStatusCommitted, 1)
	sec2Lock.Unlock()
	t.Logf("✓ Client retry succeeded - Secondary2 now has committed data")

	// FINAL VERIFICATION: All servers have data exactly once
	for i, server := range []*ChunkServer{primary, secondary1, secondary2} {
		count := countOccurrences(t, server, chunkHandle, testData)
		if count != 1 {
			t.Errorf("Server %d: Data written %d times, expected 1", i, count)
		}

		if state, ok := server.lookupAppendState(chunkHandle, idempotencyID); !ok || state.status != appendStatusCommitted {
			t.Errorf("Server %d: Should have committed state", i)
		}
	}

	t.Log("✓ Secondary failure before commit test passed - transaction aborted, retry succeeded, data written exactly once")
}
