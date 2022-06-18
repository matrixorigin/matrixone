package container

import (
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/stl"
	"github.com/stretchr/testify/assert"
)

func TestVector1(t *testing.T) {
	opts := new(Options)
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

	vec.Update(100, int64(999))
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

func TestVector2(t *testing.T) {
	vec := New[[]byte]()
	defer vec.Close()
	vec.Append([]byte("hello"))
	t.Log(vec.String())
	v := vec.Get(0)
	assert.Equal(t, "hello", string(v))
	vec.Append([]byte("world"))
	assert.Equal(t, 2, vec.Length())
	vec.Delete(0)
	assert.Equal(t, 1, vec.Length())
	v = vec.Get(0)
	assert.Equal(t, "world", string(v))
}

func TestVector3(t *testing.T) {
	vec := New[[]byte]()
	vec.Append([]byte("h1"))
	vec.Append([]byte("h2"))
	vec.Append([]byte("h3"))
	vec.Append([]byte("h4"))
	assert.Equal(t, 4, vec.Length())
	vec.Update(1, []byte("hello"))
	t.Logf("%s", vec.Get(3))
	t.Logf("%s", vec.Get(2))
	t.Logf("%s", vec.Get(1))
	t.Logf("%s", vec.Get(0))
	assert.Equal(t, "h1", string(vec.Get(0)))
	assert.Equal(t, "hello", string(vec.Get(1)))
	assert.Equal(t, "h3", string(vec.Get(2)))
	assert.Equal(t, "h4", string(vec.Get(3)))
	t.Log(vec.String())
	alloc := vec.GetAllocator()
	t.Log(stl.DefaultPool.String())
	vec.Close()
	assert.Equal(t, 0, alloc.Usage())
}
