// Struct — Go's record type. Field names, then their types.
package main

type Store struct {
	data map[string]string
}

func (s *Store) Set(key, value string) {
	s.data[key] = value
}

func (s *Store) Get(key string) (string, bool) {

	v, ok := s.data[key]

	return v, ok

}

func (s *Store) Delete(key string) {

	delete(s.data, key)

}

func NewStore() *Store {
	return &Store{data: make(map[string]string)}
}
