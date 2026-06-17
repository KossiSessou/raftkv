package main

import (
	"math/rand"
	"strings"
	"sync"
	"testing"
)

var implementations = map[string]func() Store{
	"MutexStore":   func() Store { return NewMutexStore() },
	"ShardedStore": func() Store { return NewShardedStore() },
}

func TestRaceCon(t *testing.T) {
	var wg sync.WaitGroup
	s := NewMutexStore()

	wg.Add(10)

	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()

			for i := 0; i < 100; i++ {
				s.Set("foo", "bar")
			}

		}()

	}

	wg.Wait()

}

func TestStoreGetSet(t *testing.T) {

	for name, newStore := range implementations {

		t.Run(name, func(t *testing.T) {
			tests := []struct {
				name      string
				key       string
				value     string
				wantValue string
				wantOk    bool
			}{
				{"simple set and get", "foo", "bar", "bar", true},
				{"empty key works", "", "value", "value", true},
				{"empty value", "foo", "", "", true},
				{"unicode key", "café", "value", "value", true},
				{"long string", strings.Repeat("a", 1000), "value", "value", true},
				{"key with spaces", "key space", "value", "value", true},
			}
			for _, tc := range tests {
				s := newStore()
				s.Set(tc.key, tc.value)
				got, ok := s.Get(tc.key)

				if got != tc.wantValue || ok != tc.wantOk {
					t.Errorf("Get(%q) = (%q, %v), want(%q, %v)", tc.key, got, ok, tc.wantValue, tc.wantOk)
				}

			}
		})
	}

}

func TestStoreOverwrite(t *testing.T) {

	for name, newStore := range implementations {

		t.Run(name, func(t *testing.T) {
			tests := []struct {
				name        string
				key         string
				firstValue  string
				secondValue string
				wantValue   string
				wantOk      bool
			}{
				{"overwrite same value", "foo", "bar", "bar", "bar", true},
				{"overwrite different value", "foo", "value1", "value2", "value2", true},
			}
			for _, tc := range tests {

				s := newStore()

				s.Set(tc.key, tc.firstValue)
				s.Set(tc.key, tc.secondValue)

				got, ok := s.Get(tc.key)

				if got != tc.wantValue || ok != tc.wantOk {
					t.Errorf("Get(%q) = (%q, %v), want(%q, %v)", tc.key, got, ok, tc.wantValue, tc.wantOk)
				}
			}
		})

	}
}

func TestStoreSetDeleteGet(t *testing.T) {
	for name, newStore := range implementations {
		t.Run(name, func(t *testing.T) {
			s := newStore()
			s.Set("foo", "bar")
			s.Delete("foo")

			got, ok := s.Get("foo")

			if got != "" || ok {
				t.Errorf("Get(%q) = (%q, %v), want (%q, %v)", "foo", got, ok, "", false)
			}
		})
	}
}

func TestStoreDeleteMissing(t *testing.T) {
	for name, newStore := range implementations {

		t.Run(name, func(t *testing.T) {
			s := newStore()
			s.Delete("foo")

			got, ok := s.Get("foo")

			if got != "" || ok {

				t.Errorf("Get(%q) = (%q, %v), want (%q, %v)", "foo", got, ok, "", false)
			}
		})
	}
}

func TestStoreIndependant(t *testing.T) {

	for name, newStore := range implementations {

		t.Run(name, func(t *testing.T) {
			s := newStore()

			s.Set("Alice", "value 1")
			s.Set("Bob", "value 2")

			gotAl, okAl := s.Get("Alice")
			gotBob, okBob := s.Get("Bob")

			if gotAl != "value 1" || !okAl || gotBob != "value 2" || !okBob {
				t.Errorf("Get(%q) = (%q, %v), want (%q, %v)", "Alice", gotAl, okAl, "value 1", true)
				t.Errorf("Get(%q) = (%q, %v), want (%q, %v)", "Bob", gotBob, okBob, "value 2", true)
			}

		})

	}
}

func benchStore(b *testing.B, s Store, numGoroutines, readPct int) {
	var wg sync.WaitGroup
	opsPer := b.N / numGoroutines
	keys := []string{"a", "b", "c", "d", "e", "f", "g", "h"}

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < opsPer; j++ {
				k := keys[rand.Intn(len(keys))]
				if rand.Intn(100) < readPct {
					s.Get(k)
				} else {
					s.Set(k, "v")
				}
			}
		}()
	}
	wg.Wait()
}

func BenchmarkStore(b *testing.B) {
	configs := []struct {
		name       string
		goroutines int
		readPct    int
	}{
		{"1G_50R50W", 1, 50},
		{"8G_50R50W", 8, 50},
		{"64G_50R50W", 64, 50},
		{"1G_90R10W", 1, 90},
		{"8G_90R10W", 8, 90},
		{"64G_90R10W", 64, 90},
		{"1G_10R90W", 1, 10},
		{"8G_10R90W", 8, 10},
		{"64G_10R90W", 64, 10},
	}

	for implName, newStore := range implementations {
		for _, cfg := range configs {
			b.Run(implName+"/"+cfg.name, func(b *testing.B) {
				s := newStore()
				b.ResetTimer() // exclude store creation from timing
				benchStore(b, s, cfg.goroutines, cfg.readPct)
			})
		}
	}
}
