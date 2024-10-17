// Copyright 2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package merge

import (
	"cmp"
	"encoding/csv"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"math"
	"math/rand"
	"os"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

var globalID int

type mergeEntry struct {
	id   int
	zm   index.ZM
	ts   types.TS
	size int

	generation int
}

func newMergeEntry(min, max int64, ts types.TS, size int) *mergeEntry {
	zm := index.NewZM(types.T_int64, 0)
	index.UpdateZM(zm, types.EncodeInt64(&min))
	index.UpdateZM(zm, types.EncodeInt64(&max))
	id := globalID
	globalID++
	return &mergeEntry{
		id:   id,
		zm:   zm,
		ts:   ts,
		size: size,
	}
}

type mockEntrySet struct {
	entries  []*mergeEntry
	maxValue []byte

	generation int
}

func (s *mockEntrySet) reset() {
	s.entries = s.entries[:0]
	s.maxValue = []byte{}
}

func (s *mockEntrySet) add(obj *mergeEntry) {
	s.entries = append(s.entries, obj)
	if zm := obj.zm; len(s.maxValue) == 0 || compute.Compare(s.maxValue, zm.GetMaxBuf(), zm.GetType(), zm.GetScale(), zm.GetScale()) < 0 {
		s.maxValue = zm.GetMaxBuf()
	}
}

func checkOverlaps(entries []*mergeEntry) []*mergeEntry {
	overlappingSet := make([][]*mergeEntry, 0)
	slices.SortFunc(entries, func(a, b *mergeEntry) int {
		zmA := a.zm
		zmB := b.zm
		if c := zmA.CompareMin(zmB); c != 0 {
			return c
		}
		return zmA.CompareMax(zmB)
	})
	set := mockEntrySet{entries: make([]*mergeEntry, 0), maxValue: []byte{}}
	for _, obj := range entries {
		if obj.generation != 0 {
			continue
		}
		if len(set.entries) == 0 {
			set.add(obj)
			continue
		}

		if zm := obj.zm; index.StrictlyCompareZmMaxAndMin(set.maxValue, zm.GetMinBuf(), zm.GetType(), zm.GetScale(), zm.GetScale()) > 0 {
			// zm is overlapped
			set.add(obj)
			continue
		}

		// obj is not added in the set.
		// either dismiss the set or add the set in m.overlappingObjsSet
		if len(set.entries) > 1 {
			objs := make([]*mergeEntry, len(set.entries))
			copy(objs, set.entries)
			overlappingSet = append(overlappingSet, objs)
		}

		set.reset()
		set.add(obj)
	}
	// there is still more than one entry in set.
	if len(set.entries) > 1 {
		objs := make([]*mergeEntry, len(set.entries))
		copy(objs, set.entries)
		overlappingSet = append(overlappingSet, objs)
		set.reset()
	}
	if len(overlappingSet) == 0 {
		return nil
	}

	slices.SortFunc(overlappingSet, func(a, b []*mergeEntry) int {
		return cmp.Compare(len(a), len(b))
	})

	// get the overlapping set with most objs.
	objs := overlappingSet[len(overlappingSet)-1]
	if len(objs) < 2 {
		return nil
	}
	if len(objs) > 16 {
		objs = objs[:16]
	}
	return objs
}

func createCSVWriter(filename string) (*csv.Writer, *os.File, error) {
	f, err := os.Create(filename)
	if err != nil {
		return nil, nil, err
	}
	writer := csv.NewWriter(f)
	return writer, f, nil
}

func BenchmarkMergeOverlap(b *testing.B) {
	entryChan := make(chan *mergeEntry, 1)

	go func() {
		ticker := time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
			}
			a, b := rand.Int63n(10000), rand.Int63n(10000)
			a, b = min(a, b), max(a, b)
			ts := types.BuildTS(time.Now().Unix(), 0)
			size := 128 * common.Const1MBytes

			entryChan <- newMergeEntry(a, b, ts, size)
		}
	}()

	entries := make([]*mergeEntry, 0)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	i := 0

	csvWriter, f, err := createCSVWriter("output.csv")
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()
	defer func() {
		csvWriter.Flush()
		if err = csvWriter.Error(); err != nil {
			b.Fatal(err)
		}
	}()

	for {
		select {
		case entry := <-entryChan:
			entries = append(entries, entry)
		case <-ticker.C:
		}

		if len(entries) == 0 {
			continue
		}
		fmt.Println("round", i)

		i++

		inputs := checkOverlaps(entries)
		if len(inputs) < 2 {
			record := []string{
				strconv.Itoa(i),
				strconv.Itoa(0),
			}
			err = csvWriter.Write(record)
			if err != nil {
				b.Fatal(err)
			}
			csvWriter.Flush()
			continue
		}
		for _, e := range inputs {
			entries = slices.DeleteFunc(entries, func(entry *mergeEntry) bool {
				return entry.id == e.id
			})
		}
		outputs, mergedSize := merge(inputs, 110*common.Const1MBytes)
		record := []string{
			strconv.Itoa(i),
			strconv.Itoa(mergedSize / common.Const1MBytes),
		}
		err = csvWriter.Write(record)
		if err != nil {
			b.Fatal(err)
		}
		csvWriter.Flush()
		entries = append(entries, outputs...)
	}
}

func merge(inputs []*mergeEntry, targetSize int) ([]*mergeEntry, int) {
	totalSize := 0
	maxGeneration := 0
	minValue, maxValue := int64(math.MaxInt64), int64(math.MinInt64)
	for _, input := range inputs {
		totalSize += input.size
		if input.zm.GetMin().(int64) < minValue {
			minValue = input.zm.GetMin().(int64)
		}
		if input.zm.GetMax().(int64) > maxValue {
			maxValue = input.zm.GetMax().(int64)
		}
		if input.generation > maxGeneration {
			maxGeneration = input.generation
		}
	}

	mergedSize := totalSize
	num := int64(totalSize / targetSize)
	if num == 0 {
		num = 1
	}

	interval := (maxValue - minValue) / num
	entries := make([]*mergeEntry, 0)
	for {
		if totalSize < 2*targetSize {
			entry := newMergeEntry(minValue, maxValue, types.BuildTS(time.Now().Unix(), 0), totalSize)
			entries = append(entries, entry)
			break
		}
		entry := newMergeEntry(minValue, minValue+interval, types.BuildTS(time.Now().Unix(), 0), targetSize)
		entry.generation = maxGeneration + 1
		entries = append(entries, entry)
		minValue += interval
		minValue++
		totalSize -= targetSize
	}

	return entries, mergedSize
}

func TestMerge(t *testing.T) {
	inputs := []*mergeEntry{
		newMergeEntry(0, 100, types.BuildTS(time.Now().Unix(), 0), 100*common.Const1MBytes),
		newMergeEntry(100, 200, types.BuildTS(time.Now().Unix(), 0), 110*common.Const1MBytes),
	}

	output, mergedSize := merge(inputs, 50*common.Const1MBytes)
	for i, entry := range output {
		fmt.Printf("entry %d: ", i)
		fmt.Printf("zm(%d, %d), ", entry.zm.GetMin(), entry.zm.GetMax())
		fmt.Printf("size: %d", entry.size/common.Const1MBytes)
		fmt.Printf("\n")
	}
	t.Log(mergedSize)
}
