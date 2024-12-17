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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/stretchr/testify/require"
)

func TestPool(t *testing.T) {
	addrs := make([]uint64, 10)

	m, err := mpool.NewMPool("test", 0, 0)
	require.Nil(t, err)
	mp := NewFixedBytePool(m, context.TODO(), 2, 6, 0)

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

func TestPoolSpill(t *testing.T) {

	addrs := make([]uint64, 10)
	m, err := mpool.NewMPool("test", 0, 0)
	require.Nil(t, err)
	mp := NewFixedBytePool(m, context.TODO(), 2, 6, 19)
	require.Nil(t, err)

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
	m, err := mpool.NewMPool("test", 0, 0)
	require.Nil(t, err)
	p, err := NewPartition(m, context.TODO(), 0, LOWER_BIT_MASK, 8)
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
	m, err := mpool.NewMPool("test", 0, 0)
	require.Nil(t, err)
	p, err := NewPartition(m, context.TODO(), 0, LOWER_BIT_MASK, 8)
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
	m, err := mpool.NewMPool("test", 0, 0)
	require.Nil(t, err)
	p, err := NewPartition(m, context.TODO(), 0, LOWER_BIT_MASK, 8)
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
