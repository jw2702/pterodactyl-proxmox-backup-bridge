package store

import "sync"

// KeyedMutex serializes operations that share a key (e.g. "bucket/key" or an
// uploadId) without serializing unrelated operations. bbolt itself only
// serializes individual transactions, not the multi-step
// PBS-call-then-DB-write protocols built on top of it, so callers doing
// those multi-step protocols must hold the relevant key's lock for the full
// duration.
type KeyedMutex struct {
	mu    sync.Mutex
	locks map[string]*refCountedMutex
}

type refCountedMutex struct {
	mu  sync.Mutex
	ref int
}

func NewKeyedMutex() *KeyedMutex {
	return &KeyedMutex{locks: make(map[string]*refCountedMutex)}
}

// Lock acquires the lock for key and returns an unlock function the caller
// must invoke exactly once (typically via defer).
func (k *KeyedMutex) Lock(key string) func() {
	k.mu.Lock()
	rm, ok := k.locks[key]
	if !ok {
		rm = &refCountedMutex{}
		k.locks[key] = rm
	}
	rm.ref++
	k.mu.Unlock()

	rm.mu.Lock()

	return func() {
		rm.mu.Unlock()
		k.mu.Lock()
		rm.ref--
		if rm.ref == 0 {
			delete(k.locks, key)
		}
		k.mu.Unlock()
	}
}
