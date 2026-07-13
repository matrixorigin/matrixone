// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fulltext

import (
	"context"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPool(t *testing.T) {
	addrs := make([]uint64, 10)

	fmt.Printf("%v\n", t.TempDir())
	localFS, err := fileservice.NewLocalFS(
		context.Background(),
		defines.LocalFileServiceName,
		t.TempDir(),
		fileservice.DisabledCacheConfig,
		nil,
	)
	assert.Nil(t, err)
	m := mpool.MustNewZeroNoFixed()

	proc := &process.Process{
		Base: &process.BaseProcess{
			FileService: localFS,
		},
		Ctx: context.Background(),
	}
	proc.SetMPool(m)

	mp := NewFixedBytePool(proc, 2, 6, 0)

	// total 3 partitions
	for i := 0; i < 10; i++ {
		addr, data, err := mp.NewItem()
		require.Nil(t, err)

		addrs[i] = addr

		for j := range data {
			data[j] = byte(i)
		}
		//id := GetPartitionId(addr)
		//offset := GetPartitionOffset(addr)
		//fmt.Printf("ID %d, offset %d, data = %v\n", id, offset, data)
	}

	for i, addr := range addrs {
		data, err := mp.GetItem(addr)
		require.Nil(t, err)
		//id := GetPartitionId(addr)
		//offset := GetPartitionOffset(addr)
		//fmt.Printf("ID %d, offset %d, data = %v\n", id, offset, data)

		for j := range data {
			require.Equal(t, byte(i), data[j])
			data[j] += 8
		}
	}

	for i, addr := range addrs {
		data, err := mp.GetItem(addr)
		require.Nil(t, err)
		//id := GetPartitionId(addr)
		//offset := GetPartitionOffset(addr)
		//fmt.Printf("ID %d, offset %d, data = %v\n", id, offset, data)
		for j := range data {
			require.Equal(t, byte(i+8), data[j])
		}
	}

	for _, addr := range addrs {
		err := mp.FreeItem(addr)
		require.Nil(t, err)
	}

	err = mp.FreeItem(addrs[0])
	//fmt.Println(err)
	require.NotNil(t, err)
}

// TestPoolMemAvailGate covers #25638: growing the pool by another partition also grows the
// fulltext TVF's non-spillable per-doc maps. Before creating a new block the pool estimates
// that block's footprint (docvec + map entries) and, if it will not fit in currently-available
// memory, fails cleanly instead of OOMing the CN.
func TestPoolMemAvailGate(t *testing.T) {
	origHeap := memGolang
	origTotal := memTotal
	defer func() { memGolang = origHeap; memTotal = origTotal }()

	m := mpool.MustNewZeroNoFixed()
	proc := &process.Process{Base: &process.BaseProcess{}, Ctx: context.Background()}
	proc.SetMPool(m)

	// dsize=2, partition_cap=6 -> 3 items per partition; est per new block = 6 + (6/2)*MapMemPerItem.
	mp := NewFixedBytePool(proc, 2, 6, 0)

	// ~1 TiB total, heap ~empty: the first block (3 items) is created and filled fine.
	// (partition 0 is never gated; the gate only fires when growing PAST it.)
	memTotal = func() uint64 { return 1 << 40 }
	memGolang = func() int { return 0 }
	for i := 0; i < 3; i++ {
		_, _, err := mp.NewItem()
		require.NoError(t, err)
	}

	// heap now at total (well over the 80% budget): creating the next block fails cleanly.
	memGolang = func() int { return 1 << 40 }
	_, _, err := mp.NewItem()
	require.Error(t, err)
	require.Contains(t, err.Error(), "too many documents")

	mp.Close()
}

func TestPoolSpill(t *testing.T) {

	addrs := make([]uint64, 10)
	localFS, err := fileservice.NewLocalFS(
		context.Background(),
		defines.LocalFileServiceName,
		t.TempDir(),
		fileservice.DisabledCacheConfig,
		nil,
	)
	assert.Nil(t, err)
	m := mpool.MustNewZeroNoFixed()

	proc := &process.Process{
		Base: &process.BaseProcess{
			FileService: localFS,
		},
		Ctx: context.Background(),
	}
	proc.SetMPool(m)

	mp := NewFixedBytePool(proc, 2, 6, 19)

	for i := 0; i < 10; i++ {
		addr, data, err := mp.NewItem()
		require.Nil(t, err)

		addrs[i] = addr

		for j := range data {
			data[j] = byte(i)
		}
		//id := GetPartitionId(addr)
		//offset := GetPartitionOffset(addr)
		//fmt.Printf("ID %d, offset %d, data = %v\n", id, offset, data)
	}

	//mp.Spill()

	// New 10 more items
	for i := 10; i < 20; i++ {
		addr, data, err := mp.NewItem()
		require.Nil(t, err)

		addrs = append(addrs, addr)

		for j := range data {
			data[j] = byte(i)
		}
		//id := GetPartitionId(addr)
		//offset := GetPartitionOffset(addr)
		//fmt.Printf("ID %d, offset %d, data = %v\n", id, offset, data)
	}

	// after spill and NewItem directly.  It should unspill and GetItem
	for i, addr := range addrs {
		data, err := mp.GetItem(addr)
		require.Nil(t, err)
		//id := GetPartitionId(addr)
		//offset := GetPartitionOffset(addr)
		//fmt.Printf("ID %d, offset %d, data = %v\n", id, offset, data)
		for j := range data {
			require.Equal(t, byte(i), data[j])
		}
	}

	i := 0
	it := NewFixedBytePoolIterator(mp)
	for {
		data, err := it.Next()
		require.Nil(t, err)
		if data == nil {
			// EOF
			break
		}
		//fmt.Printf("data = %v\n", data)
		for j := range data {
			require.Equal(t, byte(i), data[j])
		}
		i++
	}

	mp.Close()
	mp.Close()
	mp.Close()

}

func TestPartitionSpill(t *testing.T) {

	addrs := make([]uint64, 10)
	localFS, err := fileservice.NewLocalFS(
		context.Background(),
		defines.LocalFileServiceName,
		t.TempDir(),
		fileservice.DisabledCacheConfig,
		nil,
	)
	assert.Nil(t, err)
	mp := mpool.MustNewZeroNoFixed()

	proc := &process.Process{
		Base: &process.BaseProcess{
			FileService: localFS,
		},
		Ctx: context.Background(),
	}
	proc.SetMPool(mp)

	p, err := NewPartition(proc, 0, LOWER_BIT_MASK, 8)
	require.Nil(t, err)

	for i := 0; i < 10; i++ {
		addr, data, err := p.NewItem()
		require.Nil(t, err)

		addrs[i] = addr

		for j := range data {
			data[j] = byte(i)
		}
		//id := GetPartitionId(addr)
		//offset := GetPartitionOffset(addr)
		//fmt.Printf("ID %d, offset %d, data = %v\n", id, offset, data)
	}

	// no effect
	err = p.Unspill()
	require.Nil(t, err)

	err = p.Spill()
	require.Nil(t, err)

	err = p.Unspill()
	require.Nil(t, err)

	for i, addr := range addrs {
		data, err := p.GetItem(addr)
		require.Nil(t, err)
		//id := GetPartitionId(addr)
		//offset := GetPartitionOffset(addr)
		//fmt.Printf("ID %d, offset %d, data = %v\n", id, offset, data)

		for j := range data {
			require.Equal(t, byte(i), data[j])
		}
	}

}

func TestPartitionSpillError(t *testing.T) {

	addrs := make([]uint64, 10)

	localFS, err := fileservice.NewLocalFS(
		context.Background(),
		defines.LocalFileServiceName,
		t.TempDir(),
		fileservice.DisabledCacheConfig,
		nil,
	)
	assert.Nil(t, err)
	mp := mpool.MustNewZeroNoFixed()

	proc := &process.Process{
		Base: &process.BaseProcess{
			FileService: localFS,
		},
		Ctx: context.Background(),
	}
	proc.SetMPool(mp)

	p, err := NewPartition(proc, 0, LOWER_BIT_MASK, 8)
	require.Nil(t, err)

	for i := 0; i < 10; i++ {
		addr, data, err := p.NewItem()
		require.Nil(t, err)

		addrs[i] = addr

		for j := range data {
			data[j] = byte(i)
		}
		//id := GetPartitionId(addr)
		//offset := GetPartitionOffset(addr)
		//fmt.Printf("ID %d, offset %d, data = %v\n", id, offset, data)
	}

	err = p.Unspill()
	require.Nil(t, err)

	err = p.Spill()
	require.Nil(t, err)

	_, err = p.GetItem(addrs[0])
	require.NotNil(t, err)

}

func TestPartitionSpillError2(t *testing.T) {

	addrs := make([]uint64, 10)
	localFS, err := fileservice.NewLocalFS(
		context.Background(),
		defines.LocalFileServiceName,
		t.TempDir(),
		fileservice.DisabledCacheConfig,
		nil,
	)
	assert.Nil(t, err)
	mp := mpool.MustNewZeroNoFixed()

	proc := &process.Process{
		Base: &process.BaseProcess{
			FileService: localFS,
		},
		Ctx: context.Background(),
	}
	proc.SetMPool(mp)
	p, err := NewPartition(proc, 0, LOWER_BIT_MASK, 8)
	require.Nil(t, err)

	for i := 0; i < 10; i++ {
		addr, data, err := p.NewItem()
		require.Nil(t, err)

		addrs[i] = addr

		for j := range data {
			data[j] = byte(i)
		}
		//id := GetPartitionId(addr)
		//offset := GetPartitionOffset(addr)
		//fmt.Printf("ID %d, offset %d, data = %v\n", id, offset, data)
	}

	err = p.Unspill()
	require.Nil(t, err)

	err = p.Spill()
	require.Nil(t, err)

	_, _, err = p.NewItem()
	require.NotNil(t, err)

	p.Close()
	p.Close()
	p.Close()

}
