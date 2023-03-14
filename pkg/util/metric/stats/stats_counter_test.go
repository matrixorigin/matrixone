package stats

import (
	"github.com/stretchr/testify/assert"
	"sync/atomic"
	"testing"
)

func TestCounterLoad(t *testing.T) {
	c := Counter{}

	c.Add(1)
	assert.Equal(t, int64(1), c.Load())

	c.Add(1)
	assert.Equal(t, int64(2), c.Load())

	c.Add(10)
	assert.Equal(t, int64(12), c.Load())
}

func TestCounterLoadC(t *testing.T) {
	c := Counter{}

	c.Add(1)
	assert.Equal(t, int64(1), c.Load())

	c.Add(1)
	assert.Equal(t, int64(2), c.Load())

	c.Add(2)
	assert.Equal(t, int64(4), c.LoadC())

	assert.Equal(t, int64(0), c.LoadC())

	c.Add(2)
	assert.Equal(t, int64(6), c.Load())
}

func BenchmarkAdd(b *testing.B) {
	b.Run("atomic.Int64", func(b *testing.B) {
		c := atomic.Int64{}
		for i := 0; i < b.N; i++ {
			c.Add(int64(i))
		}
	})

	b.Run("Counter", func(b *testing.B) {
		c := Counter{}
		for i := 0; i < b.N; i++ {
			c.Add(int64(i))
		}
	})
}

func BenchmarkAddLoad(b *testing.B) {
	b.Run("atomic.Int64", func(b *testing.B) {
		c := atomic.Int64{}
		for i := 0; i < b.N; i++ {
			c.Add(1)
			if int64(i+1) != c.Load() {
				panic("Load() did not return global sum")
			}
		}
	})

	b.Run("Counter", func(b *testing.B) {
		c := Counter{}
		for i := 0; i < b.N; i++ {
			c.Add(1)
			if int64(i+1) != c.Load() {
				panic("Load() did not return global sum")
			}
		}
	})
}
