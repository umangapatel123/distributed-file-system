package master

import (
	"sync"
	"time"
)

type Lease struct {
	Primary    string
	Version    int64
	Expiration time.Time
}

type LeaseManager struct {
	mu     sync.Mutex
	leases map[string]*Lease
}

func NewLeaseManager() *LeaseManager {
	return &LeaseManager{leases: make(map[string]*Lease)}
}

func (lm *LeaseManager) Grant(handle, primary string, version int64, dur time.Duration) Lease {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	exp := time.Now().Add(dur)
	lease := &Lease{Primary: primary, Version: version, Expiration: exp}
	lm.leases[handle] = lease
	return *lease
}

func (lm *LeaseManager) Get(handle string) (Lease, bool) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	lease, ok := lm.leases[handle]
	if !ok {
		return Lease{}, false
	}
	return *lease, time.Now().Before(lease.Expiration)
}

func (lm *LeaseManager) Revoke(handle string) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	delete(lm.leases, handle)
}
