package vector

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestVector1(t *testing.T) {
	opts := new(Options[int64])
	opts.Capacity = 1
	vec := New[int64](opts)
	now := time.Now()

	for i := 0; i < 500; i++ {
		vec.Append(int64(i))
	}
	t.Log(time.Since(now))
	t.Log(vec.String())
	allocator := vec.GetAllocator()
	assert.True(t, allocator.Usage() > 0)
	now = time.Now()
	for i := 0; i < 500; i++ {
		v := vec.Get(i)
		assert.Equal(t, int64(i), v)
	}
	t.Log(time.Since(now))

	vec.Set(100, int64(999))
	v := vec.Get(100)
	assert.Equal(t, int64(999), v)

	assert.Equal(t, 500, vec.Length())
	vec.Delete(80)
	assert.Equal(t, 499, vec.Length())

	vec2 := New[int64]()
	for i := 0; i < 100; i++ {
		vec2.Append(int64(i + 1000))
	}
	vec.AppendMany(vec2.Slice()...)
	assert.Equal(t, 100+499, vec.Length())

	vec.Close()
	vec2.Close()
	assert.True(t, allocator.Usage() == 0)
}
