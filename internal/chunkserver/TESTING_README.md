# Exactly-Once Append Testing

This directory contains comprehensive test cases for the exactly-once append semantics implementation in the distributed file system.

## Overview

The tests verify that the system correctly handles various failure scenarios that could lead to duplicate writes, ensuring data is written exactly once even when retries occur.

## Test Scenarios

### 1. Lost Acknowledgement (Network Partition)
**File**: `exactly_once_test.go` - `TestExactlyOnce_LostAcknowledgement`

**Scenario**: The write succeeds on all replicas, but the confirmation doesn't reach the client.

**What Happens**:
- Client sends Append(Data, ID) to the Primary
- Primary writes and commits successfully
- Network drops the success response to the client
- Client times out and retries with the same idempotency ID

**Expected Behavior**: The system detects the retry ID and returns the previous success status without writing again.

**Verification**: Data appears exactly once in the chunk file.

---

### 2. Partial Replica Failure (Inconsistent State)
**File**: `exactly_once_test.go` - `TestExactlyOnce_PartialReplicaFailure`

**Scenario**: One replica fails during the prepare phase, causing the entire operation to be aborted.

**What Happens**:
- Primary and Replica A prepare successfully
- Replica B fails during prepare
- Primary aborts the operation on all replicas
- Client retries with the same idempotency ID

**Expected Behavior**: The healthy nodes clean up the failed partial write, and the retry succeeds with exactly-once semantics.

**Verification**: After abort and retry, data appears exactly once.

---

### 3. Primary Crash After Commit
**File**: `exactly_once_test.go` - `TestExactlyOnce_PrimaryCrashAfterCommit`

**Scenario**: The primary crashes immediately after committing but before responding to the client.

**What Happens**:
- Primary receives and commits the data
- Primary crashes before sending response
- New primary is elected (simulated by new instance)
- Client retries against new primary

**Expected Behavior**: This test demonstrates the limitation when idempotency state is lost. In a real GFS system, the master and replicas would help detect this.

**Note**: This test shows that persistent idempotency state or coordination with replicas is needed for perfect exactly-once semantics across crashes.

---

### 4. Slow Primary (False Timeout)
**File**: `exactly_once_test.go` - `TestExactlyOnce_SlowPrimary`

**Scenario**: The system works correctly but too slowly for the client's timeout.

**What Happens**:
- Client sends append request
- Primary enters a slow operation (simulated by sleep)
- Client timeout expires and triggers a retry
- Original operation eventually completes

**Expected Behavior**: Both the slow operation and the retry detect the same idempotency ID and only write data once.

**Verification**: Data appears exactly once despite concurrent slow and fast operations.

---

### 5. Concurrent Retries (Race Conditions)
**File**: `exactly_once_test.go` - `TestExactlyOnce_ConcurrentRetries`

**Scenario**: The client sends the same request twice in parallel due to aggressive retry logic.

**What Happens**:
- Client Thread A sends Append(X)
- Client spawns retry logic
- Client sends Append(X) again on a separate thread with the same idempotency ID
- Both requests arrive nearly simultaneously

**Expected Behavior**: The system's locking mechanism ensures only one write occurs, and both requests return the same offset.

**Verification**: Data written exactly once, both requests get identical responses.

---

### 6. Multiple Different Appends
**File**: `exactly_once_test.go` - `TestExactlyOnce_MultipleDistinctAppends`

**Scenario**: Multiple different append operations with different idempotency IDs.

**Purpose**: Ensures that the exactly-once mechanism doesn't prevent legitimate different appends.

**Expected Behavior**: All different appends succeed and each appears exactly once.

---

### 7. Idempotency ID Reuse After Commit
**File**: `exactly_once_test.go` - `TestExactlyOnce_IdempotencyIDReuse`

**Scenario**: Client retries with the same idempotency ID immediately after a successful commit.

**Expected Behavior**: The system returns the cached result (same offset) without performing another write.

**Verification**: Data appears exactly once, retry gets the same offset as original.

---

### 8. Stress Test - Concurrent Clients with Retries
**File**: `exactly_once_test.go` - `TestExactlyOnce_StressTest`

**Scenario**: 20 concurrent clients, each performing 3 retry attempts with the same idempotency ID.

**Expected Behavior**: All clients succeed, and each client's data appears exactly once.

**Verification**: All 20 clients' data written exactly once to the chunk.

---

## Running the Tests

### Run all exactly-once tests:
```bash
go test -v ./internal/chunkserver -run TestExactlyOnce
```

### Run a specific test:
```bash
go test -v ./internal/chunkserver -run TestExactlyOnce_LostAcknowledgement
```

### Run with timeout:
```bash
go test -v ./internal/chunkserver -run TestExactlyOnce -timeout 60s
```

### Run in short mode (skips stress test):
```bash
go test -v -short ./internal/chunkserver -run TestExactlyOnce
```

## Implementation Details

The exactly-once semantics are implemented using:

1. **Idempotency IDs**: Each append request includes a unique idempotency ID
2. **Append State Tracking**: The system maintains a history of recent append operations with their status (preparing, prepared, committed)
3. **Chunk-Level Locking**: Ensures serialized access to chunk append operations
4. **State Pruning**: Keeps the last 128 append states per chunk to limit memory usage
5. **Three-Phase Protocol**: Prepare → Commit → Acknowledge

### Key Functions

- `lookupAppendState()`: Check if an idempotency ID was already processed
- `saveAppendState()`: Record the state of an append operation
- `reserveLocalAppendLocked()`: Allocate space for the append
- `commitLocalAppendLocked()`: Write the actual data
- `abortLocalAppendLocked()`: Roll back a failed append

## Test Results Summary

✅ **Test 1**: Lost Acknowledgement - PASS  
✅ **Test 2**: Partial Replica Failure - PASS  
✅ **Test 3**: Primary Crash After Commit - PASS (demonstrates limitation)  
✅ **Test 4**: Slow Primary - PASS  
✅ **Test 5**: Concurrent Retries - PASS  
✅ **Test 6**: Multiple Distinct Appends - PASS  
✅ **Test 7**: Idempotency ID Reuse - PASS  
✅ **Test 8**: Stress Test - PASS  

## Notes

- Test 3 (Primary Crash) intentionally shows a limitation: when the primary crashes and loses all in-memory state, exactly-once semantics cannot be guaranteed without persistent storage of idempotency state or coordination with replicas.
- The stress test uses unique delimiters (`<N>`) to avoid false positives when counting data occurrences.
- All tests use temporary directories that are automatically cleaned up after execution.

## Future Improvements

1. Add persistent storage for idempotency state to handle crash scenarios
2. Implement replica-assisted recovery for better crash resilience
3. Add network partition simulation for multi-replica testing
4. Test with actual network timeouts and gRPC failures
