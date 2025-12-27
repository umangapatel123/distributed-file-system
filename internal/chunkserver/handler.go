package chunkserver

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"time"

	pb "ds-gfs-append/api/proto"
)

const appendPhaseTimeout = 5 * time.Second

func genDataID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// PushData receives streaming data and stores into in-memory buffer with provided data_id.
func (cs *ChunkServer) PushData(stream pb.ChunkServer_PushDataServer) error {
	var currentID string
	total := int64(0)
	checksum := crc32.NewIEEE()
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			if currentID == "" {
				currentID = genDataID()
			}
			resp := &pb.PushDataResponse{DataId: currentID, Size: total, Checksum: checksum.Sum32()}
			if err := stream.SendAndClose(resp); err != nil {
				return err
			}
			log.Printf("chunkserver %s: buffered data id=%s size=%d checksum=%d", cs.addr, resp.DataId, resp.Size, resp.Checksum)
			return nil
		}
		if err != nil {
			return err
		}
		if req.DataId != "" {
			currentID = req.DataId
		}
		if currentID == "" {
			currentID = genDataID()
		}
		cs.storeBuffer(currentID, req.Data)
		checksum.Write(req.Data)
		total += int64(len(req.Data))
	}
}

func (cs *ChunkServer) AppendRecord(ctx context.Context, req *pb.AppendRecordRequest) (*pb.AppendRecordResponse, error) {
	if req.GetIdempotencyId() == "" {
		return &pb.AppendRecordResponse{Success: false}, fmt.Errorf("missing idempotency id")
	}
	version, err := cs.ensureLease(req.ChunkHandle)
	if err != nil {
		return &pb.AppendRecordResponse{Success: false}, err
	}
	data, err := cs.getBuffer(req.DataId)
	if err != nil {
		return &pb.AppendRecordResponse{Success: false}, err
	}
	if len(data) == 0 {
		return &pb.AppendRecordResponse{Success: false}, fmt.Errorf("append payload is empty")
	}

	chunkLock := cs.getChunkLock(req.ChunkHandle)
	chunkLock.Lock()
	defer chunkLock.Unlock()

	if state, ok := cs.lookupAppendState(req.ChunkHandle, req.IdempotencyId); ok {
		switch state.status {
		case appendStatusCommitted:
			return cs.handleCommittedAppend(ctx, req, state)
		case appendStatusPrepared, appendStatusPreparing:
			if err := cs.abortLocalAppendLocked(req.ChunkHandle, req.IdempotencyId, state); err != nil {
				log.Printf("chunkserver %s: failed to cleanup stale append state handle=%s id=%s: %v", cs.addr, req.ChunkHandle, req.IdempotencyId, err)
			}
			cs.deleteAppendState(req.ChunkHandle, req.IdempotencyId)
		}
	}

	offset, err := cs.currentChunkSize(req.ChunkHandle)
	if err != nil {
		return &pb.AppendRecordResponse{Success: false}, err
	}

	state := &appendRecordState{
		offset:       offset,
		length:       int64(len(data)),
		originalSize: offset,
		status:       appendStatusPreparing,
	}
	cs.saveAppendState(req.ChunkHandle, req.IdempotencyId, state)

	if err := cs.reserveLocalAppendLocked(req.ChunkHandle, state); err != nil {
		cs.deleteAppendState(req.ChunkHandle, req.IdempotencyId)
		return &pb.AppendRecordResponse{Success: false}, err
	}
	cs.updateAppendStateStatus(req.ChunkHandle, req.IdempotencyId, appendStatusPrepared, 0)

	reservedLength := int64(len(data))
	preparedSecondaries := make([]string, 0, len(req.Secondaries))
	for _, loc := range req.Secondaries {
		if loc == nil || loc.Address == "" {
			continue
		}
		if err := cs.forwardAppendPhase(ctx, loc.Address, req, pb.AppendPhase_APPEND_PHASE_PREPARE, offset, reservedLength, version); err != nil {
			log.Printf("chunkserver %s: prepare failed for secondary %s chunk=%s: %v", cs.addr, loc.Address, req.ChunkHandle, err)
			cs.nullifySecondaries(ctx, preparedSecondaries, req, offset, reservedLength, version)
			if abortErr := cs.abortLocalAppendLocked(req.ChunkHandle, req.IdempotencyId, state); abortErr != nil {
				log.Printf("chunkserver %s: local abort after prepare failure chunk=%s: %v", cs.addr, req.ChunkHandle, abortErr)
			}
			cs.deleteAppendState(req.ChunkHandle, req.IdempotencyId)
			return &pb.AppendRecordResponse{Success: false}, fmt.Errorf("prepare failed: %w", err)
		}
		preparedSecondaries = append(preparedSecondaries, loc.Address)
	}

	for _, addr := range preparedSecondaries {
		if err := cs.forwardAppendPhase(ctx, addr, req, pb.AppendPhase_APPEND_PHASE_COMMIT, offset, reservedLength, version); err != nil {
			log.Printf("chunkserver %s: commit failed for secondary %s chunk=%s: %v", cs.addr, addr, req.ChunkHandle, err)
			cs.nullifySecondaries(ctx, preparedSecondaries, req, offset, reservedLength, version)
			if abortErr := cs.abortLocalAppendLocked(req.ChunkHandle, req.IdempotencyId, state); abortErr != nil {
				log.Printf("chunkserver %s: local abort after commit failure chunk=%s: %v", cs.addr, req.ChunkHandle, abortErr)
			}
			cs.deleteAppendState(req.ChunkHandle, req.IdempotencyId)
			return &pb.AppendRecordResponse{Success: false}, fmt.Errorf("commit failed: %w", err)
		}
	}

	if err := cs.commitLocalAppendLocked(req.ChunkHandle, offset, data, req.ChunkVersion); err != nil {
		log.Printf("chunkserver %s: local commit failed chunk=%s: %v", cs.addr, req.ChunkHandle, err)
		cs.nullifySecondaries(ctx, preparedSecondaries, req, offset, reservedLength, version)
		if abortErr := cs.abortLocalAppendLocked(req.ChunkHandle, req.IdempotencyId, state); abortErr != nil {
			log.Printf("chunkserver %s: local abort after local commit failure chunk=%s: %v", cs.addr, req.ChunkHandle, abortErr)
		}
		cs.deleteAppendState(req.ChunkHandle, req.IdempotencyId)
		return &pb.AppendRecordResponse{Success: false}, err
	}

	cs.updateAppendStateStatus(req.ChunkHandle, req.IdempotencyId, appendStatusCommitted, version)
	cs.touchAppendState(req.ChunkHandle, req.IdempotencyId)
	return &pb.AppendRecordResponse{Success: true, Offset: offset, CommittedVersion: version}, nil
}

func (cs *ChunkServer) handleCommittedAppend(ctx context.Context, req *pb.AppendRecordRequest, state *appendRecordState) (*pb.AppendRecordResponse, error) {
	if err := cs.ensureSecondariesConsistent(ctx, req, state); err != nil {
		return &pb.AppendRecordResponse{Success: false}, err
	}
	return &pb.AppendRecordResponse{
		Success:          true,
		Offset:           state.offset,
		CommittedVersion: state.committedVersion,
	}, nil
}

func (cs *ChunkServer) ensureSecondariesConsistent(ctx context.Context, req *pb.AppendRecordRequest, state *appendRecordState) error {
	if len(req.Secondaries) == 0 {
		return nil
	}
	missing, err := cs.findInconsistentSecondaries(ctx, req.ChunkHandle, req.IdempotencyId, req.Secondaries)
	if err != nil {
		return err
	}
	if len(missing) == 0 {
		return nil
	}
	return cs.replayAppendOnSecondaries(ctx, req, state, missing)
}

func (cs *ChunkServer) findInconsistentSecondaries(ctx context.Context, handle, id string, secondaries []*pb.ChunkLocation) ([]string, error) {
	missing := make([]string, 0, len(secondaries))
	seen := make(map[string]struct{})
	for _, loc := range secondaries {
		if loc == nil || loc.Address == "" {
			continue
		}
		if _, ok := seen[loc.Address]; ok {
			continue
		}
		seen[loc.Address] = struct{}{}
		completed, err := cs.remoteIdempotencyStatus(ctx, loc.Address, handle, id)
		if err != nil {
			return nil, fmt.Errorf("check idempotency on %s failed: %w", loc.Address, err)
		}
		if !completed {
			missing = append(missing, loc.Address)
		}
	}
	return missing, nil
}

func (cs *ChunkServer) remoteIdempotencyStatus(ctx context.Context, addr, handle, id string) (bool, error) {
	conn, err := grpcDial(addr)
	if err != nil {
		return false, err
	}
	defer conn.Close()
	client := pb.NewChunkServerClient(conn)
	statusCtx, cancel := context.WithTimeout(ctx, appendPhaseTimeout)
	defer cancel()
	resp, err := client.CheckIdempotency(statusCtx, &pb.IdempotencyStatusRequest{ChunkHandle: handle, IdempotencyId: id})
	if err != nil {
		return false, err
	}
	return resp.GetCompleted(), nil
}

func (cs *ChunkServer) replayAppendOnSecondaries(ctx context.Context, req *pb.AppendRecordRequest, state *appendRecordState, targets []string) error {
	if len(targets) == 0 {
		return nil
	}
	prepared := make([]string, 0, len(targets))
	for _, addr := range targets {
		if err := cs.forwardAppendPhase(ctx, addr, req, pb.AppendPhase_APPEND_PHASE_PREPARE, state.offset, state.length, state.committedVersion); err != nil {
			cs.nullifySecondaries(ctx, prepared, req, state.offset, state.length, state.committedVersion)
			return fmt.Errorf("replay prepare failed for %s: %w", addr, err)
		}
		prepared = append(prepared, addr)
	}
	for _, addr := range prepared {
		if err := cs.forwardAppendPhase(ctx, addr, req, pb.AppendPhase_APPEND_PHASE_COMMIT, state.offset, state.length, state.committedVersion); err != nil {
			cs.nullifySecondaries(ctx, prepared, req, state.offset, state.length, state.committedVersion)
			return fmt.Errorf("replay commit failed for %s: %w", addr, err)
		}
	}
	return nil
}

func (cs *ChunkServer) ForwardAppend(ctx context.Context, req *pb.ForwardAppendRequest) (*pb.ForwardAppendResponse, error) {
	if req.GetIdempotencyId() == "" {
		return &pb.ForwardAppendResponse{Success: false}, fmt.Errorf("missing idempotency id")
	}
	chunkLock := cs.getChunkLock(req.ChunkHandle)
	chunkLock.Lock()
	defer chunkLock.Unlock()

	var err error
	switch req.GetPhase() {
	case pb.AppendPhase_APPEND_PHASE_PREPARE:
		err = cs.handleForwardPrepare(req)
	case pb.AppendPhase_APPEND_PHASE_COMMIT:
		err = cs.handleForwardCommit(req)
	case pb.AppendPhase_APPEND_PHASE_ABORT:
		err = cs.handleForwardAbort(req)
	default:
		err = fmt.Errorf("unknown append phase: %v", req.GetPhase())
	}
	if err != nil {
		return &pb.ForwardAppendResponse{Success: false}, err
	}
	return &pb.ForwardAppendResponse{Success: true}, nil
}

func (cs *ChunkServer) forwardAppendPhase(ctx context.Context, addr string, req *pb.AppendRecordRequest, phase pb.AppendPhase, offset int64, length int64, version int64) error {
	conn, err := grpcDial(addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	client := pb.NewChunkServerClient(conn)
	phaseCtx, cancel := context.WithTimeout(ctx, appendPhaseTimeout)
	defer cancel()
	resp, err := client.ForwardAppend(phaseCtx, &pb.ForwardAppendRequest{
		ChunkHandle:    req.ChunkHandle,
		DataId:         req.DataId,
		Offset:         offset,
		ChunkVersion:   version,
		Phase:          phase,
		ReservedLength: length,
		IdempotencyId:  req.IdempotencyId,
	})
	if err != nil {
		return err
	}
	if resp == nil || !resp.Success {
		return fmt.Errorf("secondary %s rejected phase %s", addr, phase.String())
	}
	return nil
}

func (cs *ChunkServer) nullifySecondaries(ctx context.Context, addresses []string, req *pb.AppendRecordRequest, offset int64, length int64, version int64) {
	if len(addresses) == 0 {
		return
	}
	seen := make(map[string]struct{})
	for _, addr := range addresses {
		if addr == "" {
			continue
		}
		if _, ok := seen[addr]; ok {
			continue
		}
		seen[addr] = struct{}{}
		if err := cs.forwardAppendPhase(ctx, addr, req, pb.AppendPhase_APPEND_PHASE_ABORT, offset, length, version); err != nil {
			log.Printf("chunkserver %s: abort phase failed for secondary %s chunk=%s: %v", cs.addr, addr, req.ChunkHandle, err)
		}
	}
}

func (cs *ChunkServer) reserveLocalAppendLocked(handle string, state *appendRecordState) error {
	if state == nil {
		return fmt.Errorf("missing append state for reserve")
	}
	if state.length <= 0 {
		return fmt.Errorf("invalid append length %d", state.length)
	}
	if err := truncateChunkFile(cs.dataDir, handle, state.originalSize); err != nil {
		return err
	}
	if err := writeZeroAt(cs.dataDir, handle, state.offset, state.length); err != nil {
		return err
	}
	cs.setChunkSize(handle, state.offset+state.length)
	log.Printf("chunkserver %s: prepared chunk=%s offset=%d length=%d", cs.addr, handle, state.offset, state.length)
	return nil
}

func (cs *ChunkServer) commitLocalAppendLocked(handle string, offset int64, data []byte, version int64) error {
	if len(data) == 0 {
		return fmt.Errorf("commit payload is empty")
	}
	if err := writeAt(cs.dataDir, handle, offset, data); err != nil {
		return err
	}
	cs.setChunkSize(handle, offset+int64(len(data)))
	cs.setChunkVersion(handle, version)
	log.Printf("chunkserver %s: committed chunk=%s offset=%d bytes=%d", cs.addr, handle, offset, len(data))
	return nil
}

func (cs *ChunkServer) abortLocalAppendLocked(handle, id string, state *appendRecordState) error {
	original := int64(0)
	if state != nil {
		original = state.originalSize
	} else {
		size, err := cs.currentChunkSize(handle)
		if err != nil {
			return err
		}
		original = size
	}
	if original < 0 {
		original = 0
	}
	if err := truncateChunkFile(cs.dataDir, handle, original); err != nil {
		return err
	}
	cs.setChunkSize(handle, original)
	if id != "" {
		cs.touchAppendState(handle, id)
	}
	log.Printf("chunkserver %s: aborted chunk=%s restored_size=%d", cs.addr, handle, original)
	return nil
}

func (cs *ChunkServer) handleForwardPrepare(req *pb.ForwardAppendRequest) error {
	if req.GetReservedLength() <= 0 {
		return fmt.Errorf("invalid reserved length")
	}
	if state, ok := cs.lookupAppendState(req.ChunkHandle, req.IdempotencyId); ok {
		switch state.status {
		case appendStatusCommitted:
			return nil
		case appendStatusPrepared:
			if state.offset == req.Offset && state.length == req.ReservedLength {
				return nil
			}
			if err := cs.abortLocalAppendLocked(req.ChunkHandle, req.IdempotencyId, state); err != nil {
				return err
			}
			cs.deleteAppendState(req.ChunkHandle, req.IdempotencyId)
		default:
			if err := cs.abortLocalAppendLocked(req.ChunkHandle, req.IdempotencyId, state); err != nil {
				return err
			}
			cs.deleteAppendState(req.ChunkHandle, req.IdempotencyId)
		}
	}

	currentSize, err := cs.currentChunkSize(req.ChunkHandle)
	if err != nil {
		return err
	}
	if currentSize != req.Offset {
		if currentSize == req.Offset+req.ReservedLength {
			state := &appendRecordState{
				offset:       req.Offset,
				length:       req.ReservedLength,
				originalSize: req.Offset,
				status:       appendStatusPrepared,
			}
			cs.saveAppendState(req.ChunkHandle, req.IdempotencyId, state)
			cs.setChunkSize(req.ChunkHandle, currentSize)
			return nil
		}
		return fmt.Errorf("secondary chunk size mismatch: have %d expected %d", currentSize, req.Offset)
	}

	state := &appendRecordState{
		offset:       req.Offset,
		length:       req.ReservedLength,
		originalSize: currentSize,
		status:       appendStatusPreparing,
	}
	cs.saveAppendState(req.ChunkHandle, req.IdempotencyId, state)
	if err := cs.reserveLocalAppendLocked(req.ChunkHandle, state); err != nil {
		cs.deleteAppendState(req.ChunkHandle, req.IdempotencyId)
		return err
	}
	cs.updateAppendStateStatus(req.ChunkHandle, req.IdempotencyId, appendStatusPrepared, 0)
	return nil
}

func (cs *ChunkServer) handleForwardCommit(req *pb.ForwardAppendRequest) error {
	state, ok := cs.lookupAppendState(req.ChunkHandle, req.IdempotencyId)
	if ok {
		if state.status == appendStatusCommitted {
			return nil
		}
	} else {
		return fmt.Errorf("commit received without prepare for id %s", req.IdempotencyId)
	}
	if state.offset != req.Offset {
		return fmt.Errorf("commit offset mismatch: have %d want %d", req.Offset, state.offset)
	}
	data, err := cs.getBuffer(req.DataId)
	if err != nil {
		return err
	}
	if int64(len(data)) == 0 {
		return fmt.Errorf("commit payload empty on secondary")
	}
	if err := cs.commitLocalAppendLocked(req.ChunkHandle, state.offset, data, req.ChunkVersion); err != nil {
		return err
	}
	cs.updateAppendStateStatus(req.ChunkHandle, req.IdempotencyId, appendStatusCommitted, req.ChunkVersion)
	return nil
}

func (cs *ChunkServer) handleForwardAbort(req *pb.ForwardAppendRequest) error {
	if state, ok := cs.lookupAppendState(req.ChunkHandle, req.IdempotencyId); ok {
		if err := cs.abortLocalAppendLocked(req.ChunkHandle, req.IdempotencyId, state); err != nil {
			return err
		}
		cs.deleteAppendState(req.ChunkHandle, req.IdempotencyId)
		return nil
	}
	// No explicit state recorded; best-effort truncate to requested offset if needed.
	currentSize, err := cs.currentChunkSize(req.ChunkHandle)
	if err != nil {
		return err
	}
	if currentSize > req.Offset {
		if err := truncateChunkFile(cs.dataDir, req.ChunkHandle, req.Offset); err != nil {
			return err
		}
		cs.setChunkSize(req.ChunkHandle, req.Offset)
	}
	return nil
}

func (cs *ChunkServer) WriteChunk(ctx context.Context, req *pb.WriteChunkRequest) (*pb.WriteChunkResponse, error) {
	if req.Offset < 0 {
		return &pb.WriteChunkResponse{Success: false}, fmt.Errorf("invalid offset")
	}
	var (
		version int64
		err     error
	)
	if req.GetBypassLease() {
		version = req.GetChunkVersion()
		if version == 0 {
			cs.mu.Lock()
			version = cs.chunkVersion[req.ChunkHandle]
			cs.mu.Unlock()
		}
	} else {
		version, err = cs.ensureLease(req.ChunkHandle)
		if err != nil {
			return &pb.WriteChunkResponse{Success: false}, err
		}
	}
	data, err := cs.getBuffer(req.DataId)
	if err != nil {
		return &pb.WriteChunkResponse{Success: false}, err
	}
	if req.Offset+int64(len(data)) > cs.chunkSize {
		return &pb.WriteChunkResponse{Success: false}, fmt.Errorf("write exceeds chunk size")
	}
	if req.GetBypassLease() && req.Offset == 0 {
		if err := truncateChunkFile(cs.dataDir, req.ChunkHandle, 0); err != nil {
			return &pb.WriteChunkResponse{Success: false}, err
		}
	}
	log.Printf("chunkserver %s: write chunk=%s offset=%d bytes=%d", cs.addr, req.ChunkHandle, req.Offset, len(data))
	if err := writeAt(cs.dataDir, req.ChunkHandle, req.Offset, data); err != nil {
		return &pb.WriteChunkResponse{Success: false}, err
	}
	cs.mu.Lock()
	updated := false
	if req.Offset+int64(len(data)) > cs.chunkSizes[req.ChunkHandle] {
		cs.chunkSizes[req.ChunkHandle] = req.Offset + int64(len(data))
		updated = true
	}
	cs.mu.Unlock()
	if updated {
		cs.markStateDirty()
	}
	applyVersion := req.ChunkVersion
	if applyVersion == 0 {
		applyVersion = version
	}
	cs.setChunkVersion(req.ChunkHandle, applyVersion)
	for _, loc := range req.Secondaries {
		conn, err := grpcDial(loc.Address)
		if err != nil {
			log.Printf("forward write dial failed %s: %v", loc.Address, err)
			return &pb.WriteChunkResponse{Success: false}, nil
		}
		c := pb.NewChunkServerClient(conn)
		_, err = c.ForwardWrite(ctx, &pb.ForwardWriteRequest{ChunkHandle: req.ChunkHandle, DataId: req.DataId, Offset: req.Offset, ChunkVersion: version})
		conn.Close()
		if err != nil {
			log.Printf("forward write failed to %s: %v", loc.Address, err)
			return &pb.WriteChunkResponse{Success: false}, nil
		}
	}
	return &pb.WriteChunkResponse{Success: true}, nil
}

func (cs *ChunkServer) ForwardWrite(ctx context.Context, req *pb.ForwardWriteRequest) (*pb.ForwardWriteResponse, error) {
	data, err := cs.getBuffer(req.DataId)
	if err != nil {
		return &pb.ForwardWriteResponse{Success: false}, err
	}
	if req.Offset+int64(len(data)) > cs.chunkSize {
		return &pb.ForwardWriteResponse{Success: false}, fmt.Errorf("write exceeds chunk size")
	}
	log.Printf("chunkserver %s: forward write chunk=%s offset=%d bytes=%d", cs.addr, req.ChunkHandle, req.Offset, len(data))
	if err := writeAt(cs.dataDir, req.ChunkHandle, req.Offset, data); err != nil {
		return &pb.ForwardWriteResponse{Success: false}, err
	}
	cs.mu.Lock()
	updated := false
	if req.Offset+int64(len(data)) > cs.chunkSizes[req.ChunkHandle] {
		cs.chunkSizes[req.ChunkHandle] = req.Offset + int64(len(data))
		updated = true
	}
	cs.mu.Unlock()
	if updated {
		cs.markStateDirty()
	}
	cs.setChunkVersion(req.ChunkHandle, req.ChunkVersion)
	return &pb.ForwardWriteResponse{Success: true}, nil
}

func (cs *ChunkServer) ReadChunk(ctx context.Context, req *pb.ReadChunkRequest) (*pb.ReadChunkResponse, error) {
	if req.Offset < 0 {
		return nil, fmt.Errorf("invalid offset")
	}
	data, err := readAt(cs.dataDir, req.ChunkHandle, req.Offset, req.Length)
	if err != nil {
		return nil, err
	}
	log.Printf("chunkserver %s: read chunk=%s offset=%d length=%d actual=%d", cs.addr, req.ChunkHandle, req.Offset, req.Length, len(data))
	return &pb.ReadChunkResponse{Data: data}, nil
}

func (cs *ChunkServer) CheckIdempotency(ctx context.Context, req *pb.IdempotencyStatusRequest) (*pb.IdempotencyStatusResponse, error) {
	if req.GetChunkHandle() == "" || req.GetIdempotencyId() == "" {
		return nil, fmt.Errorf("missing chunk handle or idempotency id")
	}
	if state, ok := cs.lookupAppendState(req.GetChunkHandle(), req.GetIdempotencyId()); ok && state.status == appendStatusCommitted {
		return &pb.IdempotencyStatusResponse{Completed: true}, nil
	}
	return &pb.IdempotencyStatusResponse{Completed: false}, nil
}
