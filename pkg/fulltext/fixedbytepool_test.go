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
	"github.com/stretchr/testify/require"
)

func TestPool(t *testing.T) {
	addrs := make([]uint64, 10)

	m, err := mpool.NewMPool("test", 0, 0)
	mp := NewFixedBytePool(m, context.TODO(), 2, 6, 0)

	// total 3 partitions
	for i := 0; i < 10; i++ {
		addr, data, err := mp.NewItem()
		require.Nil(t, err)

		addrs[i] = addr
		id := GetPartitionId(addr)
		offset := GetPartitionOffset(addr)

		for j := range data {
			data[j] = byte(i)
		}
		fmt.Printf("ID %d, offset %d, data = %v\n", id, offset, data)
	}

	for _, addr := range addrs {
		data, err := mp.GetItem(addr)
		require.Nil(t, err)
		id := GetPartitionId(addr)
		offset := GetPartitionOffset(addr)
		fmt.Printf("ID %d, offset %d, data = %v\n", id, offset, data)

		for i := range data {
			data[i] = 111
		}
	}

	for _, addr := range addrs {
		data, err := mp.GetItem(addr)
		require.Nil(t, err)
		id := GetPartitionId(addr)
		offset := GetPartitionOffset(addr)
		fmt.Printf("ID %d, offset %d, data = %v\n", id, offset, data)
	}

	for _, addr := range addrs {
		err := mp.FreeItem(addr)
		require.Nil(t, err)
	}

	err = mp.FreeItem(addrs[0])
	fmt.Println(err)
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
		id := GetPartitionId(addr)
		offset := GetPartitionOffset(addr)

		for j := range data {
			data[j] = byte(i)
		}
		fmt.Printf("ID %d, offset %d, data = %v\n", id, offset, data)
	}

	//mp.Spill()

	// New 10 more items
	for i := 10; i < 20; i++ {
		addr, data, err := mp.NewItem()
		require.Nil(t, err)

		addrs = append(addrs, addr)
		id := GetPartitionId(addr)
		offset := GetPartitionOffset(addr)

		for j := range data {
			data[j] = byte(i)
		}
		fmt.Printf("ID %d, offset %d, data = %v\n", id, offset, data)
	}

	// after spill and NewItem directly.  It should unspill and GetItem
	for _, addr := range addrs {
		data, err := mp.GetItem(addr)
		require.Nil(t, err)
		id := GetPartitionId(addr)
		offset := GetPartitionOffset(addr)
		fmt.Printf("ID %d, offset %d, data = %v\n", id, offset, data)
	}

	it := NewFixedBytePoolIterator(mp)
	for {
		data, err := it.Next()
		require.Nil(t, err)
		if data == nil {
			// EOF
			break
		}
		fmt.Printf("data = %v\n", data)
	}

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
		id := GetPartitionId(addr)
		offset := GetPartitionOffset(addr)

		for j := range data {
			data[j] = byte(i)
		}
		fmt.Printf("ID %d, offset %d, data = %v\n", id, offset, data)
	}

	// no effect
	err = p.Unspill()
	require.Nil(t, err)

	err = p.Spill()
	require.Nil(t, err)

	err = p.Unspill()
	require.Nil(t, err)

	for _, addr := range addrs {
		data, err := p.GetItem(addr)
		require.Nil(t, err)
		id := GetPartitionId(addr)
		offset := GetPartitionOffset(addr)
		fmt.Printf("ID %d, offset %d, data = %v\n", id, offset, data)
	}

}

func TestPartitionSpill2(t *testing.T) {

	addrs := make([]uint64, 10)
	m, err := mpool.NewMPool("test", 0, 0)
	require.Nil(t, err)
	p, err := NewPartition(m, context.TODO(), 0, LOWER_BIT_MASK, 8)
	require.Nil(t, err)

	for i := 0; i < 10; i++ {
		addr, data, err := p.NewItem()
		require.Nil(t, err)

		addrs[i] = addr
		id := GetPartitionId(addr)
		offset := GetPartitionOffset(addr)

		for j := range data {
			data[j] = byte(i)
		}
		fmt.Printf("ID %d, offset %d, data = %v\n", id, offset, data)
	}

	err = p.Unspill()
	require.Nil(t, err)

	err = p.Spill()
	require.Nil(t, err)

	_, err = p.GetItem(addrs[0])
	require.NotNil(t, err)

}

func TestPartitionSpill3(t *testing.T) {

	addrs := make([]uint64, 10)
	m, err := mpool.NewMPool("test", 0, 0)
	require.Nil(t, err)
	p, err := NewPartition(m, context.TODO(), 0, LOWER_BIT_MASK, 8)
	require.Nil(t, err)

	for i := 0; i < 10; i++ {
		addr, data, err := p.NewItem()
		require.Nil(t, err)

		addrs[i] = addr
		id := GetPartitionId(addr)
		offset := GetPartitionOffset(addr)

		for j := range data {
			data[j] = byte(i)
		}
		fmt.Printf("ID %d, offset %d, data = %v\n", id, offset, data)
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
