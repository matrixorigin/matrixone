package common

import (
	"github.com/panjf2000/ants/v2"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func doAlloc(size uint64) []byte {
	return make([]byte, size)
}

func TestPool(t *testing.T) {
	mp := NewMempool(M * K * 8)
	n1 := mp.Alloc(65)
	assert.Equal(t, 1, n1.PageIdx())
	n2 := mp.Alloc(257)
	assert.Equal(t, 2, n2.PageIdx())
	assert.Equal(t, uint64(n1.Size()+n2.Size()), mp.Usage())

	mp.Free(n1)
	assert.Equal(t, uint64(n2.Size()), mp.Usage())
	mp.Free(n2)
	assert.Equal(t, uint64(0), mp.Usage())

	size := K * 4
	p, _ := ants.NewPool(20)
	var wg sync.WaitGroup

	now := time.Now()
	// hm := host.New(1 << 48)
	// gm := guest.New(1<<48, hm)
	// proc := process.New(gm, mempool.New(1<<48, 8))

	// for i := 0; i < 1024*512; i++ {
	// 	wg.Add(1)
	// 	f := func() {
	// 		defer wg.Done()
	// 		buf, err := proc.Alloc(int64(size))
	// 		if err != nil {
	// 			panic(err)
	// 		}
	// 		proc.Free(buf)
	// 	}
	// 	p.Submit(f)
	// }
	// wg.Wait()
	// t.Logf("1. Takes %s, proc size: %d", time.Since(now), proc.Size())
	// now = time.Now()

	// for i := 0; i < 1024*2; i++ {
	for i := 0; i < 1024*1024; i++ {
		wg.Add(1)
		f := func() {
			defer wg.Done()
			n := mp.Alloc(size)
			if n != nil {
				mp.Free(n)
			}
		}
		p.Submit(f)
	}
	wg.Wait()
	t.Logf("Takes %s", time.Since(now))
	assert.Equal(t, uint64(0), mp.Usage())
	t.Log(mp.String())
	n := mp.Alloc(2 * M)
	if n != nil {
		assert.Equal(t, uint64(1), mp.other)
		mp.Free(n)
		assert.Equal(t, uint64(0), mp.other)
	}

	for i := 0; i < 10; i++ {
		wg.Add(1)
		f := func() {
			defer wg.Done()
			quota := mp.ApplyQuota(size)
			mp.Free(quota)
		}
		p.Submit(f)
	}
	wg.Wait()
	t.Log(mp.String())
}
