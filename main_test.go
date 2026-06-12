package main

import "testing"

func TestAdd(t *testing.T) {
	got := Add(3, 2)
	want := 5

	if got != want {
		t.Errorf("Add(2, 3) = %d; want %d", got, want)
	}
}

func BenchmarkAdd(b *testing.B) {

	for i := 0; i < b.N; i++ {
		Add(2, 3)

	}
}
