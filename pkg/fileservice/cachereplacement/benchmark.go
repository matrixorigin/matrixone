package cachereplacement

import (
	"testing"
)

func BenchmarkSet(
	newPolicy func(capacity int64) Policy,
	b *testing.B,
) {

	const capacity = 1024
	l := newPolicy(capacity)

	for i := 0; i < b.N; i++ {
		l.Set(i%capacity, i, 1)
	}
}

func BenchmarkParallelSet(newPolicy func(capacity int64) Policy,
	b *testing.B,
) {
	const capacity = 1024
	l := newPolicy(capacity)

	b.RunParallel(func(pb *testing.PB) {
		for i := 0; pb.Next(); i++ {
			l.Set(i%capacity, i, 1)
		}
	})
}

func BenchmarkParallelSetOrGet(newPolicy func(capacity int64) Policy,
	b *testing.B,
) {
	const capacity = 1024
	l := newPolicy(capacity)

	b.RunParallel(func(pb *testing.PB) {
		for i := 0; pb.Next(); i++ {
			l.Set(i%capacity, i, 1)
			if i%2 == 0 {
				l.Get(i % capacity)
			}
		}
	})
}
