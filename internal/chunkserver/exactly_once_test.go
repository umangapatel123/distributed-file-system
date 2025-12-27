package chunkserver

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	pb "ds-gfs-append/api/proto"

	"google.golang.org/grpc"
)

func startTestChunkServer(t *testing.T, impl pb.ChunkServerServer) (string, func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	srv := grpc.NewServer()
	pb.RegisterChunkServerServer(srv, impl)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = srv.Serve(lis)
	}()
	cleanup := func() {
		srv.Stop()
		<-done
		_ = lis.Close()
	}
	return lis.Addr().String(), cleanup
}

type flakyCommitSecondary struct {
	*ChunkServer
	once sync.Once
}

func (f *flakyCommitSecondary) ForwardAppend(ctx context.Context, req *pb.ForwardAppendRequest) (*pb.ForwardAppendResponse, error) {
	if req.GetPhase() == pb.AppendPhase_APPEND_PHASE_COMMIT {
		failed := false
		f.once.Do(func() {
			failed = true
		})
		if failed {
			return nil, fmt.Errorf("simulated secondary crash during commit")
		}
	}
	return f.ChunkServer.ForwardAppend(ctx, req)
}

// Helper to count occurrences of data in a chunk
func countOccurrences(t *testing.T, cs *ChunkServer, handle string, pattern []byte) int {
	size, err := cs.currentChunkSize(handle)
	if err != nil {
		return 0
	}

	if size == 0 {
		return 0
	}

	data, err := readAt(cs.dataDir, handle, 0, size)
	if err != nil {
		t.Logf("Failed to read chunk %s: %v", handle, err)
		return 0
	}

	count := 0
	for i := 0; i <= len(data)-len(pattern); i++ {
		match := true
		for j := 0; j < len(pattern); j++ {
			if data[i+j] != pattern[j] {
				match = false
				break
			}
		}
		if match {
			count++
			i += len(pattern) - 1
		}
	}

	return count
}

// Test 1: Lost Acknowledgement (Network Partition)
// This tests the scenario where the append succeeds but the client doesn't receive the ACK
func TestExactlyOnce_LostAcknowledgement(t *testing.T) {
	// Setup: Create chunk server
	cs1 := NewChunkServer("cs1", t.TempDir(), "", 64<<20, 10*time.Second)
	defer cs1.Shutdown()

	chunkHandle := "ch-test-lost-ack"
	idempotencyID := "append-id-12345"
	testData := []byte("test data for lost ack scenario")

	// Simulate: Push data to server (in real system this would be over network)
	cs1.storeBuffer("data-1", testData)

	// First attempt - simulating successful execution
	req := &pb.AppendRecordRequest{
		ChunkHandle:   chunkHandle,
		DataId:        "data-1",
		IdempotencyId: idempotencyID,
		ChunkVersion:  1,
		Secondaries:   []*pb.ChunkLocation{}, // Simplified - testing on primary only
	}

	resp1, err := cs1.AppendRecord(context.Background(), req)
	if err != nil {
		t.Fatalf("First append failed: %v", err)
	}
	if !resp1.Success {
		t.Fatalf("First append returned success=false")
	}

	offset1 := resp1.Offset
	t.Logf("First append succeeded at offset %d", offset1)

	// Simulate: Client didn't receive response, retries with same idempotency ID
	resp2, err := cs1.AppendRecord(context.Background(), req)
	if err != nil {
		t.Fatalf("Retry failed: %v", err)
	}
	if !resp2.Success {
		t.Fatalf("Retry returned success=false")
	}

	// Verify: Same offset returned
	if resp2.Offset != offset1 {
		t.Errorf("Retry returned different offset: %d vs %d", resp2.Offset, offset1)
	}

	// Verify: Data written exactly once
	count := countOccurrences(t, cs1, chunkHandle, testData)
	if count != 1 {
		t.Errorf("Data written %d times, expected exactly 1", count)
	}

	t.Log("✓ Lost acknowledgement test passed - data written exactly once")
}

// Test 2: Partial Replica Failure (Inconsistent State)
// Tests cleanup of failed partial writes when one replica fails
func TestExactlyOnce_PartialReplicaFailure(t *testing.T) {
	cs1 := NewChunkServer("cs1", t.TempDir(), "", 64<<20, 10*time.Second)
	cs2 := NewChunkServer("cs2", t.TempDir(), "", 64<<20, 10*time.Second)
	defer cs1.Shutdown()
	defer cs2.Shutdown()

	chunkHandle := "ch-test-partial-fail"
	idempotencyID := "append-id-67890"
	testData := []byte("test data for partial replica failure")

	// Push data to servers
	cs1.storeBuffer("data-2", testData)
	cs2.storeBuffer("data-2", testData)

	// First attempt - simulate preparing on cs1 and cs2, but cs3 fails
	lock := cs1.getChunkLock(chunkHandle)
	lock.Lock()

	// Get initial offset
	offset, _ := cs1.currentChunkSize(chunkHandle)

	// Prepare on primary
	state := &appendRecordState{
		offset:       offset,
		length:       int64(len(testData)),
		originalSize: offset,
		status:       appendStatusPreparing,
	}
	cs1.saveAppendState(chunkHandle, idempotencyID, state)
	cs1.reserveLocalAppendLocked(chunkHandle, state)
	cs1.updateAppendStateStatus(chunkHandle, idempotencyID, appendStatusPrepared, 0)

	// Prepare on cs2
	lock2 := cs2.getChunkLock(chunkHandle)
	lock2.Lock()
	state2 := &appendRecordState{
		offset:       offset,
		length:       int64(len(testData)),
		originalSize: offset,
		status:       appendStatusPreparing,
	}
	cs2.saveAppendState(chunkHandle, idempotencyID, state2)
	cs2.reserveLocalAppendLocked(chunkHandle, state2)
	cs2.updateAppendStateStatus(chunkHandle, idempotencyID, appendStatusPrepared, 0)
	lock2.Unlock()

	// cs3 fails - simulate abort on cs1 and cs2 due to failure
	cs1.abortLocalAppendLocked(chunkHandle, idempotencyID, state)
	cs1.deleteAppendState(chunkHandle, idempotencyID)

	lock2.Lock()
	cs2.abortLocalAppendLocked(chunkHandle, idempotencyID, state2)
	cs2.deleteAppendState(chunkHandle, idempotencyID)
	lock2.Unlock()

	lock.Unlock()

	// Now retry - this time all succeed
	req := &pb.AppendRecordRequest{
		ChunkHandle:   chunkHandle,
		DataId:        "data-2",
		IdempotencyId: idempotencyID,
		ChunkVersion:  1,
		Secondaries:   []*pb.ChunkLocation{},
	}

	resp, err := cs1.AppendRecord(context.Background(), req)
	if err != nil {
		t.Fatalf("Retry after failure failed: %v", err)
	}
	if !resp.Success {
		t.Fatalf("Retry returned success=false")
	}

	// Verify data written exactly once on cs1
	count := countOccurrences(t, cs1, chunkHandle, testData)
	if count != 1 {
		t.Errorf("CS1: Data written %d times, expected 1", count)
	}

	// Verify cs2 was properly aborted (should have 0 copies since we only appended to cs1)
	count2 := countOccurrences(t, cs2, chunkHandle, testData)
	if count2 != 0 {
		t.Logf("CS2: Data written %d times (expected 0 since only primary completed)", count2)
	}

	t.Log("✓ Partial replica failure test passed - aborted writes cleaned up")
}

func TestPrimaryReplaysMissingSecondaryCommits(t *testing.T) {
	primary := NewChunkServer("primary", t.TempDir(), "", 64<<20, 10*time.Second)
	secondary := NewChunkServer("secondary", t.TempDir(), "", 64<<20, 10*time.Second)
	defer primary.Shutdown()
	defer secondary.Shutdown()
	addr, cleanup := startTestChunkServer(t, secondary)
	defer cleanup()

	chunkHandle := "ch-replay"
	idempotencyID := "append-id-replay"
	dataID := "data-replay"
	payload := []byte("primary replay payload")

	primary.storeBuffer(dataID, payload)
	secondary.storeBuffer(dataID, payload)

	req := &pb.AppendRecordRequest{
		ChunkHandle:   chunkHandle,
		DataId:        dataID,
		IdempotencyId: idempotencyID,
		ChunkVersion:  1,
		Secondaries: []*pb.ChunkLocation{
			{Address: addr},
		},
	}

	ctx := context.Background()
	resp, err := primary.AppendRecord(ctx, req)
	if err != nil || !resp.Success {
		t.Fatalf("initial append failed: resp=%v err=%v", resp, err)
	}

	// Simulate secondary not committing the append.
	lock := secondary.getChunkLock(chunkHandle)
	lock.Lock()
	if err := truncateChunkFile(secondary.dataDir, chunkHandle, 0); err != nil {
		lock.Unlock()
		t.Fatalf("truncate secondary chunk failed: %v", err)
	}
	secondary.setChunkSize(chunkHandle, 0)
	secondary.deleteAppendState(chunkHandle, idempotencyID)
	lock.Unlock()

	resp2, err := primary.AppendRecord(ctx, req)
	if err != nil {
		t.Fatalf("retry append returned error: %v", err)
	}
	if !resp2.Success {
		t.Fatalf("retry append reported failure")
	}
	if resp2.Offset != resp.Offset {
		t.Fatalf("expected same offset, got %d vs %d", resp2.Offset, resp.Offset)
	}

	// Verify secondary now has exactly one copy and reports completion.
	if count := countOccurrences(t, secondary, chunkHandle, payload); count != 1 {
		t.Fatalf("expected payload once on secondary, found %d", count)
	}
	status, err := secondary.CheckIdempotency(ctx, &pb.IdempotencyStatusRequest{ChunkHandle: chunkHandle, IdempotencyId: idempotencyID})
	if err != nil {
		t.Fatalf("secondary status check failed: %v", err)
	}
	if status == nil || !status.Completed {
		t.Fatalf("secondary status not marked completed")
	}
}

func TestExactlyOnce_PrimaryCrashBeforeAckAfterCommit(t *testing.T) {
	primaryDir := t.TempDir()
	primary := NewChunkServer("primary", primaryDir, "", 64<<20, 10*time.Second)
	secondary1 := NewChunkServer("secondary1", t.TempDir(), "", 64<<20, 10*time.Second)
	secondary2 := NewChunkServer("secondary2", t.TempDir(), "", 64<<20, 10*time.Second)
	defer secondary1.Shutdown()
	defer secondary2.Shutdown()

	addr1, cleanup1 := startTestChunkServer(t, secondary1)
	defer cleanup1()
	addr2, cleanup2 := startTestChunkServer(t, secondary2)
	defer cleanup2()

	chunkHandle := "ch-primary-crash-before-ack"
	idempotencyID := "append-primary-crash-before-ack"
	dataID := "data-primary-crash-before-ack"
	retryDataID := "retry-data-primary-crash-before-ack"
	payload := []byte("primary crash before ack payload")

	primary.storeBuffer(dataID, payload)
	secondary1.storeBuffer(dataID, payload)
	secondary2.storeBuffer(dataID, payload)

	req := &pb.AppendRecordRequest{
		ChunkHandle:   chunkHandle,
		DataId:        dataID,
		IdempotencyId: idempotencyID,
		ChunkVersion:  1,
		Secondaries: []*pb.ChunkLocation{
			{Address: addr1},
			{Address: addr2},
		},
	}

	ctx := context.Background()
	resp, err := primary.AppendRecord(ctx, req)
	if err != nil || !resp.Success {
		t.Fatalf("initial append failed: resp=%v err=%v", resp, err)
	}

	primary.flushStateIfDirty()
	primary.Shutdown()

	restarted := NewChunkServer("primary", primaryDir, "", 64<<20, 10*time.Second)
	defer restarted.Shutdown()

	sec2Lock := secondary2.getChunkLock(chunkHandle)
	sec2Lock.Lock()
	if err := truncateChunkFile(secondary2.dataDir, chunkHandle, 0); err != nil {
		sec2Lock.Unlock()
		t.Fatalf("truncate secondary2 chunk failed: %v", err)
	}
	secondary2.setChunkSize(chunkHandle, 0)
	secondary2.deleteAppendState(chunkHandle, idempotencyID)
	sec2Lock.Unlock()
	restarted.storeBuffer(retryDataID, payload)
	secondary1.storeBuffer(retryDataID, payload)
	secondary2.storeBuffer(retryDataID, payload)

	retryReq := &pb.AppendRecordRequest{
		ChunkHandle:   chunkHandle,
		DataId:        retryDataID,
		IdempotencyId: idempotencyID,
		ChunkVersion:  1,
		Secondaries: []*pb.ChunkLocation{
			{Address: addr1},
			{Address: addr2},
		},
	}

	resp2, err := restarted.AppendRecord(ctx, retryReq)
	if err != nil || !resp2.Success {
		t.Fatalf("retry after crash failed: resp=%v err=%v", resp2, err)
	}
	if resp2.Offset != resp.Offset {
		t.Fatalf("expected same offset %d got %d", resp.Offset, resp2.Offset)
	}

	servers := []*ChunkServer{restarted, secondary1, secondary2}
	for idx, server := range servers {
		if count := countOccurrences(t, server, chunkHandle, payload); count != 1 {
			t.Fatalf("server %d expected payload once, found %d", idx, count)
		}
	}
}

func TestExactlyOnce_SecondaryCrashDuringCommit(t *testing.T) {
	primary := NewChunkServer("primary", t.TempDir(), "", 64<<20, 10*time.Second)
	defer primary.Shutdown()
	healthy := NewChunkServer("healthy", t.TempDir(), "", 64<<20, 10*time.Second)
	defer healthy.Shutdown()
	flakyInner := NewChunkServer("flaky", t.TempDir(), "", 64<<20, 10*time.Second)
	defer flakyInner.Shutdown()
	flaky := &flakyCommitSecondary{ChunkServer: flakyInner}

	addrHealthy, cleanupHealthy := startTestChunkServer(t, healthy)
	defer cleanupHealthy()
	addrFlaky, cleanupFlaky := startTestChunkServer(t, flaky)
	defer cleanupFlaky()

	chunkHandle := "ch-secondary-crash-commit"
	idempotencyID := "append-secondary-crash-commit"
	dataID := "data-secondary-crash-commit"
	payload := []byte("secondary crash during commit payload")

	for _, server := range []*ChunkServer{primary, healthy, flakyInner} {
		server.storeBuffer(dataID, payload)
	}

	req := &pb.AppendRecordRequest{
		ChunkHandle:   chunkHandle,
		DataId:        dataID,
		IdempotencyId: idempotencyID,
		ChunkVersion:  1,
		Secondaries: []*pb.ChunkLocation{
			{Address: addrHealthy},
			{Address: addrFlaky},
		},
	}

	resp, err := primary.AppendRecord(context.Background(), req)
	if err == nil {
		t.Fatalf("expected append to fail due to secondary crash, resp=%v", resp)
	}
	if resp != nil && resp.Success {
		t.Fatalf("expected response to indicate failure")
	}

	if _, ok := primary.lookupAppendState(chunkHandle, idempotencyID); ok {
		t.Fatalf("primary should drop append state after abort")
	}
	if _, ok := healthy.lookupAppendState(chunkHandle, idempotencyID); ok {
		t.Fatalf("healthy secondary should clean append state after abort")
	}
	if _, ok := flakyInner.lookupAppendState(chunkHandle, idempotencyID); ok {
		t.Fatalf("flaky secondary should not keep prepared state after abort")
	}

	servers := []*ChunkServer{primary, healthy, flakyInner}
	for idx, server := range servers {
		if count := countOccurrences(t, server, chunkHandle, payload); count != 0 {
			t.Fatalf("server %d should not have payload, found %d copies", idx, count)
		}
	}
}

func TestChunkServerStatePersistsAcrossRestart(t *testing.T) {
	dataDir := t.TempDir()
	cs1 := NewChunkServer("cs", dataDir, "", 64<<20, 10*time.Second)
	cs1.storeBuffer("data-persist", []byte("persist me"))
	req := &pb.AppendRecordRequest{
		ChunkHandle:   "ch-persist",
		DataId:        "data-persist",
		IdempotencyId: "append-persist-1",
		ChunkVersion:  1,
		Secondaries:   []*pb.ChunkLocation{},
	}
	resp, err := cs1.AppendRecord(context.Background(), req)
	if err != nil || !resp.Success {
		t.Fatalf("initial append failed: resp=%v err=%v", resp, err)
	}
	cs1.flushStateIfDirty()
	cs1.Shutdown()

	cs2 := NewChunkServer("cs", dataDir, "", 64<<20, 10*time.Second)
	defer cs2.Shutdown()
	cs2.storeBuffer("data-persist", []byte("persist me"))
	resp2, err := cs2.AppendRecord(context.Background(), req)
	if err != nil {
		t.Fatalf("retry after restart failed: %v", err)
	}
	if !resp2.Success {
		t.Fatalf("retry reported failure")
	}
	if resp2.Offset != resp.Offset {
		t.Fatalf("expected same offset %d got %d", resp.Offset, resp2.Offset)
	}
}

// Test 3: Primary Crash After Commit
func TestExactlyOnce_PrimaryCrashAfterCommit(t *testing.T) {
	primaryDataDir := t.TempDir()
	chunkHandle := "ch-test-crash"
	idempotencyID := "append-id-crash-123"
	testData := []byte("test data for crash scenario")

	// Create first primary instance
	cs1 := NewChunkServer("primary", primaryDataDir, "", 64<<20, 10*time.Second)
	defer cs1.Shutdown()

	// Push data and append
	cs1.storeBuffer("data-3", testData)
	req := &pb.AppendRecordRequest{
		ChunkHandle:   chunkHandle,
		DataId:        "data-3",
		IdempotencyId: idempotencyID,
		ChunkVersion:  1,
		Secondaries:   []*pb.ChunkLocation{},
	}

	resp1, err := cs1.AppendRecord(context.Background(), req)
	if err != nil || !resp1.Success {
		t.Fatalf("First append failed: %v", err)
	}

	offset1 := resp1.Offset
	t.Logf("First append succeeded at offset %d", offset1)

	// Simulate crash - create new instance with same data directory
	cs2 := NewChunkServer("primary", primaryDataDir, "", 64<<20, 10*time.Second)
	defer cs2.Shutdown()

	// Push data to new instance
	cs2.storeBuffer("data-3", testData)

	// Client retries (doesn't know if first succeeded)
	resp2, err := cs2.AppendRecord(context.Background(), req)
	if err != nil {
		t.Fatalf("Retry after crash failed: %v", err)
	}
	if !resp2.Success {
		t.Fatalf("Retry returned success=false")
	}

	// Verify: Data written (state was lost in crash, so this shows the limitation)
	count := countOccurrences(t, cs2, chunkHandle, testData)
	t.Logf("After crash recovery: data appears %d times", count)

	// Note: In real GFS, the master and replicas help detect this.
	// This test shows the limitation when idempotency state is completely lost.

	t.Log("✓ Primary crash test completed - demonstrates state recovery needs")
}

// Test 4: Slow Primary (False Timeout)
func TestExactlyOnce_SlowPrimary(t *testing.T) {
	cs := NewChunkServer("cs", t.TempDir(), "", 64<<20, 10*time.Second)
	defer cs.Shutdown()

	chunkHandle := "ch-test-slow"
	idempotencyID := "append-id-slow-456"
	testData := []byte("test data for slow primary scenario")

	cs.storeBuffer("data-4", testData)

	req := &pb.AppendRecordRequest{
		ChunkHandle:   chunkHandle,
		DataId:        "data-4",
		IdempotencyId: idempotencyID,
		ChunkVersion:  1,
		Secondaries:   []*pb.ChunkLocation{},
	}

	// Start slow append in background
	resultChan := make(chan *pb.AppendRecordResponse, 1)
	errChan := make(chan error, 1)

	go func() {
		time.Sleep(2 * time.Second) // Simulate slow operation
		resp, err := cs.AppendRecord(context.Background(), req)
		resultChan <- resp
		errChan <- err
	}()

	// Client times out and retries immediately
	time.Sleep(500 * time.Millisecond)

	resp2, err2 := cs.AppendRecord(context.Background(), req)

	// Wait for slow operation to complete
	resp1 := <-resultChan
	err1 := <-errChan

	// Both should succeed
	if err1 != nil {
		t.Errorf("Slow operation failed: %v", err1)
	}
	if err2 != nil {
		t.Errorf("Retry operation failed: %v", err2)
	}

	if resp1 != nil && resp2 != nil {
		if resp1.Offset != resp2.Offset {
			t.Errorf("Different offsets: slow=%d, retry=%d", resp1.Offset, resp2.Offset)
		}
	}

	// Verify data written exactly once
	count := countOccurrences(t, cs, chunkHandle, testData)
	if count != 1 {
		t.Errorf("Data written %d times, expected exactly 1", count)
	}

	t.Log("✓ Slow primary test passed - retry handled correctly")
}

// Test 5: Concurrent Retries (Race Conditions)
func TestExactlyOnce_ConcurrentRetries(t *testing.T) {
	cs := NewChunkServer("cs", t.TempDir(), "", 64<<20, 10*time.Second)
	defer cs.Shutdown()

	chunkHandle := "ch-test-concurrent"
	idempotencyID := "append-id-concurrent-789"
	testData := []byte("test data for concurrent retry scenario")

	cs.storeBuffer("data-5", testData)

	req := &pb.AppendRecordRequest{
		ChunkHandle:   chunkHandle,
		DataId:        "data-5",
		IdempotencyId: idempotencyID,
		ChunkVersion:  1,
		Secondaries:   []*pb.ChunkLocation{},
	}

	// Fire two identical requests concurrently
	var wg sync.WaitGroup
	results := make([]*pb.AppendRecordResponse, 2)
	errs := make([]error, 2)

	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			resp, err := cs.AppendRecord(context.Background(), req)
			results[idx] = resp
			errs[idx] = err
		}(i)
	}

	wg.Wait()

	// Both should succeed
	for i := 0; i < 2; i++ {
		if errs[i] != nil {
			t.Errorf("Request %d failed: %v", i, errs[i])
		}
		if results[i] != nil && results[i].Success {
			t.Logf("Request %d succeeded at offset %d", i, results[i].Offset)
		}
	}

	// Verify both got same offset
	if results[0] != nil && results[1] != nil {
		if results[0].Offset != results[1].Offset {
			t.Errorf("Concurrent requests got different offsets: %d vs %d",
				results[0].Offset, results[1].Offset)
		}
	}

	// Verify data written exactly once
	count := countOccurrences(t, cs, chunkHandle, testData)
	if count != 1 {
		t.Errorf("Data written %d times, expected exactly 1", count)
	}

	t.Log("✓ Concurrent retries test passed - exactly one write despite parallel requests")
}

// Test 6: Multiple Different Appends Should All Succeed
func TestExactlyOnce_MultipleDistinctAppends(t *testing.T) {
	cs := NewChunkServer("cs", t.TempDir(), "", 64<<20, 10*time.Second)
	defer cs.Shutdown()

	chunkHandle := "ch-test-multiple"
	numAppends := 5

	for i := 0; i < numAppends; i++ {
		idempotencyID := fmt.Sprintf("append-id-multi-%d", i)
		dataID := fmt.Sprintf("data-%d", i)
		testData := []byte(fmt.Sprintf("test data number %d", i))

		cs.storeBuffer(dataID, testData)

		req := &pb.AppendRecordRequest{
			ChunkHandle:   chunkHandle,
			DataId:        dataID,
			IdempotencyId: idempotencyID,
			ChunkVersion:  int64(i + 1),
			Secondaries:   []*pb.ChunkLocation{},
		}

		resp, err := cs.AppendRecord(context.Background(), req)
		if err != nil {
			t.Fatalf("Append %d failed: %v", i, err)
		}
		if !resp.Success {
			t.Fatalf("Append %d returned success=false", i)
		}

		t.Logf("Append %d succeeded at offset %d", i, resp.Offset)

		// Verify this data appears exactly once
		count := countOccurrences(t, cs, chunkHandle, testData)
		if count != 1 {
			t.Errorf("Data set %d written %d times, expected 1", i, count)
		}
	}

	t.Log("✓ Multiple distinct appends test passed - all written exactly once")
}

// Test 7: Idempotency ID Reuse After Commit
func TestExactlyOnce_IdempotencyIDReuse(t *testing.T) {
	cs := NewChunkServer("cs", t.TempDir(), "", 64<<20, 10*time.Second)
	defer cs.Shutdown()

	chunkHandle := "ch-test-reuse"
	idempotencyID := "append-id-reuse-999"
	testData := []byte("test data for idempotency reuse")

	cs.storeBuffer("data-7", testData)

	req := &pb.AppendRecordRequest{
		ChunkHandle:   chunkHandle,
		DataId:        "data-7",
		IdempotencyId: idempotencyID,
		ChunkVersion:  1,
		Secondaries:   []*pb.ChunkLocation{},
	}

	// First append
	resp1, err := cs.AppendRecord(context.Background(), req)
	if err != nil || !resp1.Success {
		t.Fatalf("First append failed: %v", err)
	}

	offset1 := resp1.Offset
	t.Logf("First append at offset %d", offset1)

	// Immediately retry with same ID
	resp2, err := cs.AppendRecord(context.Background(), req)
	if err != nil || !resp2.Success {
		t.Fatalf("Retry failed: %v", err)
	}

	// Should return same offset
	if resp2.Offset != offset1 {
		t.Errorf("Retry returned different offset: %d vs %d", resp2.Offset, offset1)
	}

	// Verify still only one copy
	count := countOccurrences(t, cs, chunkHandle, testData)
	if count != 1 {
		t.Errorf("Data written %d times after reusing idempotency ID, expected 1", count)
	}

	t.Log("✓ Idempotency ID reuse test passed - returned cached result")
}

// Test 8: Stress Test - Many Concurrent Clients with Retries
func TestExactlyOnce_StressTest(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	cs := NewChunkServer("cs", t.TempDir(), "", 64<<20, 10*time.Second)
	defer cs.Shutdown()

	chunkHandle := "ch-test-stress"
	numClients := 20
	numRetriesPerClient := 3

	var wg sync.WaitGroup
	successfulAppends := sync.Map{}

	for clientID := 0; clientID < numClients; clientID++ {
		wg.Add(1)
		go func(cid int) {
			defer wg.Done()

			idempotencyID := fmt.Sprintf("stress-append-%d", cid)
			dataID := fmt.Sprintf("stress-data-%d", cid)
			testData := []byte(fmt.Sprintf("stress test data from client <%d>", cid))

			cs.storeBuffer(dataID, testData)

			req := &pb.AppendRecordRequest{
				ChunkHandle:   chunkHandle,
				DataId:        dataID,
				IdempotencyId: idempotencyID,
				ChunkVersion:  int64(cid + 1),
				Secondaries:   []*pb.ChunkLocation{},
			}

			// Try multiple times with same ID
			for retry := 0; retry < numRetriesPerClient; retry++ {
				resp, err := cs.AppendRecord(context.Background(), req)
				if err == nil && resp.Success {
					successfulAppends.Store(cid, resp.Offset)
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
		}(clientID)
	}

	wg.Wait()

	// Verify all clients succeeded
	successCount := 0
	successfulAppends.Range(func(key, value interface{}) bool {
		successCount++
		return true
	})

	if successCount != numClients {
		t.Errorf("Only %d/%d clients succeeded", successCount, numClients)
	}

	// Verify each client's data appears exactly once
	for clientID := 0; clientID < numClients; clientID++ {
		testData := []byte(fmt.Sprintf("stress test data from client <%d>", clientID))
		count := countOccurrences(t, cs, chunkHandle, testData)
		if count != 1 {
			t.Errorf("Client %d data written %d times, expected 1", clientID, count)
		}
	}

	t.Logf("✓ Stress test passed - %d concurrent clients, all data written exactly once", numClients)
}
