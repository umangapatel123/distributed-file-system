package master

import (
	"log"
	"time"

	pb "ds-gfs-append/api/proto"
)

func (m *MasterServer) checkReplicationStatus() {
	cutoff := time.Now().Add(-replicaLivenessTimeout)
	handles := m.meta.ListChunkHandles()
	target := m.replicationFactor
	if target <= 0 {
		target = 3
	}
	for _, handle := range handles {
		healthy, stale := m.meta.ReplicaHealth(handle, cutoff)
		if len(stale) > 0 {
			for _, addr := range stale {
				if m.meta.MarkReplicaStale(handle, addr) {
					log.Printf("master: replica %s for chunk %s marked stale", addr, handle)
					m.enqueueDeleteCommand(handle, []string{addr})
				}
			}
		}
		count := len(healthy)
		if count == 0 {
			continue
		}
		if count < target {
			m.scheduleReplication(handle, healthy)
		} else if count > target {
			for _, addr := range healthy[target:] {
				log.Printf("master: trimming extra replica %s for chunk %s", addr, handle)
				m.enqueueDeleteCommand(handle, []string{addr})
			}
		}
	}
}

func (m *MasterServer) scheduleReplication(handle string, healthy []string) {
	if len(healthy) == 0 {
		return
	}
	exclude := make(map[string]struct{}, len(healthy))
	for _, addr := range healthy {
		exclude[addr] = struct{}{}
	}
	target := m.selectReplicationTarget(handle, exclude)
	if target == "" {
		return
	}
	source := healthy[0]
	cmd := &pb.HeartBeatCommand{
		Type:   pb.CommandType_COMMAND_REPLICATE_CHUNK,
		Chunk:  handle,
		Target: target,
	}
	m.commandMu.Lock()
	m.commandQueue[source] = append(m.commandQueue[source], cmd)
	m.commandMu.Unlock()
	log.Printf("master: scheduled replication chunk=%s from %s to %s", handle, source, target)
}
