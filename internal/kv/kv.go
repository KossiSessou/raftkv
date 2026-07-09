package kv

import (
	"sync"
)

const numShards = 16

type Store interface {
	Set(key, value string)
	Get(key string) (string, bool)
	Delete(ket string)
}

// Struct — Go's record type. Field names, then their
// types.
type MutexStore struct {
	data map[string]string

	mu sync.RWMutex
}

type ShardedStore struct {
	shards []*shard
}

type shard struct {
	mu sync.RWMutex

	data map[string]string
}

func (s *MutexStore) Set(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
}

func (s *MutexStore) Get(key string) (string, bool) {

	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key]

	return v, ok

}

func (s *MutexStore) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, key)

}

func NewMutexStore() *MutexStore {
	return &MutexStore{data: make(map[string]string)}
}

func hash(s string) uint32 {
	var h uint32 = 2166136261
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= 16777619
	}
	return h
}

func (s *ShardedStore) getShard(key string) *shard {
	h := hash(key)

	return s.shards[h&(uint32(len(s.shards))-1)]
}

func NewShardedStore() *ShardedStore {

	shards := make([]*shard, numShards)

	for i := range shards {

		shards[i] = &shard{data: make(map[string]string)}
	}

	return &ShardedStore{shards: shards}

}

func (s *ShardedStore) Get(key string) (string, bool) {

	sh := s.getShard(key)

	sh.mu.RLock()
	defer sh.mu.RUnlock()
	val, ok := sh.data[key]

	return val, ok
}

func (s *ShardedStore) Set(key, value string) {

	sh := s.getShard(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	sh.data[key] = value

}

func (s *ShardedStore) Delete(key string) {

	sh := s.getShard(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	delete(sh.data, key)
}
