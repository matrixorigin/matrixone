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

package simulation

import (
	"bufio"
	"cmp"
	"encoding/csv"
	"fmt"
	"golang.org/x/exp/constraints"
	"math"
	"math/rand"
	"os"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/spf13/cobra"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/merge"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"gonum.org/v1/gonum/stat"
)

var MergeSimulationCmd = &cobra.Command{
	Use: "merge-simulation",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("Simulation is running background...")
		fmt.Println("Please input 'a' to check performance or 'p' to print object status.")
		fmt.Println("Press Ctrl-C to quit.")
		maxValue, err := cmd.Flags().GetInt64("max")
		if err != nil {
			panic(err)
		}

		distribution, err := cmd.Flags().GetString("new_entry_distribution")
		if err != nil {
			panic(err)
		}

		distributionArg, err := cmd.Flags().GetFloat64("new_entry_distribution_arg")
		if err != nil {
			panic(err)
		}

		entryIntervalFactory := func() time.Duration {
			return time.Duration(rand.ExpFloat64()*distributionArg) * time.Millisecond
		}

		switch distribution {
		case "M":
		// Poisson Process
		// default
		case "D":
			// constant
			entryIntervalFactory = func() time.Duration {
				return time.Duration(distributionArg) * time.Millisecond
			}
		}

		mergeInterval, err := cmd.Flags().GetDuration("merge_interval")
		if err != nil {
			panic(err)
		}

		writeToCSV, err := cmd.Flags().GetBool("write_to_csv")
		if err != nil {
			panic(err)
		}

		sim(maxValue, mergeInterval, entryIntervalFactory, writeToCSV)
	},
}

func init() {
	MergeSimulationCmd.Flags().Int64("max", 100000, "set max value of all ranges")
	MergeSimulationCmd.Flags().StringP("new_entry_distribution", "d", "M", "distribution of entry arrival in Kendall's notation")
	MergeSimulationCmd.Flags().Float64P("new_entry_distribution_arg", "a", 100, "arguments for distribution")
	MergeSimulationCmd.Flags().DurationP("merge_interval", "i", 100*time.Millisecond, "set merge interval in ms")
	MergeSimulationCmd.Flags().BoolP("write_to_csv", "c", false, "write result to csv file")
}

type entry struct {
	id   *objectio.ObjectId
	zm   index.ZM
	size int
}

type lockedEntries struct {
	sync.RWMutex

	entries []*entry
}

func newLockedEntries() *lockedEntries {
	return &lockedEntries{
		entries: make([]*entry, 0),
	}
}

func (l *lockedEntries) append(e ...*entry) {
	l.Lock()
	defer l.Unlock()
	l.entries = append(l.entries, e...)
}

func (l *lockedEntries) calculateHits(r int64) []float64 {
	l.RLock()
	defer l.RUnlock()

	hits := make([]float64, r)

	for _, e := range l.entries {
		for i := e.zm.GetMin().(int64); i < e.zm.GetMax().(int64); i++ {
			hits[i] += 1
		}
	}
	slices.DeleteFunc(hits, func(f float64) bool {
		return f == 0
	})

	return hits
}

func (l *lockedEntries) size() int {
	l.RLock()
	defer l.RUnlock()
	return len(l.entries)
}

func (l *lockedEntries) remove(del func(*entry) bool) {
	l.Lock()
	defer l.Unlock()
	l.entries = slices.DeleteFunc(l.entries, del)
}

func (l *lockedEntries) segments() [4][]*entry {
	l.RLock()
	defer l.RUnlock()
	entries := l.entries
	segments := make(map[objectio.Segmentid][]*entry)
	for _, e := range entries {
		segments[*e.id.Segment()] = append(segments[*e.id.Segment()], e)
	}

	leveledObjects := [4][]*entry{}

	for _, segment := range segments {
		level := merge.SegLevel(len(segment))
		for _, e := range segment {
			leveledObjects[level] = append(leveledObjects[level], e)
		}
	}

	return leveledObjects
}

func newEntry(min, max int64, size int) *entry {
	zm := index.NewZM(types.T_int64, 0)
	index.UpdateZM(zm, types.EncodeInt64(&min))
	index.UpdateZM(zm, types.EncodeInt64(&max))
	return &entry{
		id:   objectio.NewObjectid(),
		zm:   zm,
		size: size,
	}
}

func newEntryWithSegmentID(
	segmentID *objectio.Segmentid, offset uint16,
	min, max int64, size int) *entry {
	id := objectio.NewObjectidWithSegmentIDAndNum(segmentID, offset)
	zm := index.NewZM(types.T_int64, 0)
	index.UpdateZM(zm, types.EncodeInt64(&min))
	index.UpdateZM(zm, types.EncodeInt64(&max))

	return &entry{
		id:   id,
		zm:   zm,
		size: size,
	}
}

type entrySet struct {
	entries  []*entry
	maxValue []byte
}

func (s *entrySet) reset() {
	s.entries = s.entries[:0]
	s.maxValue = []byte{}
}

func (s *entrySet) add(obj *entry) {
	s.entries = append(s.entries, obj)
	if zm := obj.zm; len(s.maxValue) == 0 || compute.Compare(s.maxValue, zm.GetMaxBuf(), zm.GetType(), zm.GetScale(), zm.GetScale()) < 0 {
		s.maxValue = zm.GetMaxBuf()
	}
}

func (l *lockedEntries) checkOverlaps() [4][]*entry {

	leveledEntries := l.segments()
	leveledOutputs := [4][]*entry{}

	for i, entries := range leveledEntries {
		overlappingSet := make([][]*entry, 0)
		slices.SortFunc(entries, func(a, b *entry) int {
			if c := a.zm.CompareMin(b.zm); c != 0 {
				return c
			}
			return a.zm.CompareMax(b.zm)
		})
		set := entrySet{entries: make([]*entry, 0), maxValue: []byte{}}
		for _, obj := range entries {
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
				objs := make([]*entry, len(set.entries))
				copy(objs, set.entries)
				overlappingSet = append(overlappingSet, objs)
			}

			set.reset()
			set.add(obj)
		}
		// there is still more than one entry in set.
		if len(set.entries) > 1 {
			objs := make([]*entry, len(set.entries))
			copy(objs, set.entries)
			overlappingSet = append(overlappingSet, objs)
			set.reset()
		}
		if len(overlappingSet) == 0 {
			continue
		}

		slices.SortFunc(overlappingSet, func(a, b []*entry) int {
			return cmp.Compare(len(a), len(b))
		})

		// get the overlapping set with most objs.
		objs := overlappingSet[len(overlappingSet)-1]
		if len(objs) < 2 {
			continue
		}
		leveledOutputs[i] = append(leveledOutputs[i], objs...)
	}
	return leveledOutputs
}

type recentRecords[T constraints.Integer | constraints.Float] struct {
	sync.RWMutex

	size      int
	records   []T
	maxRecord T
}

func newRecentRecords[T constraints.Integer | constraints.Float](size int) recentRecords[T] {
	return recentRecords[T]{
		size:    size,
		records: make([]T, 0, size),
	}
}

func (r *recentRecords[T]) add(record T) {
	r.Lock()
	if len(r.records) == r.size {
		r.records = r.records[1:]
	}
	r.records = append(r.records, record)
	if record > r.maxRecord {
		r.maxRecord = record
	}
	r.Unlock()
}

func (r *recentRecords[T]) reset() {
	r.Lock()
	r.records = r.records[:0]
	r.Unlock()
}

func (r *recentRecords[T]) mean() T {
	r.RLock()
	defer r.RUnlock()
	if len(r.records) == 0 {
		return T(0)
	}
	sum := T(0)
	for _, record := range r.records {
		sum += record
	}
	return sum / T(len(r.records))
}

func (r *recentRecords[T]) getMax() T {
	r.RLock()
	defer r.RUnlock()
	return r.maxRecord
}

func createCSVWriter(filename string) (*csv.Writer, *os.File, error) {
	f, err := os.Create(filename)
	if err != nil {
		return nil, nil, err
	}
	writer := csv.NewWriter(f)
	return writer, f, nil
}

func sim(maxValue int64, mergeInterval time.Duration, entryIntervalFactory func() time.Duration, writeToCSV bool) {
	entryChan := make(chan *entry, 1)

	go func() {
		interval := entryIntervalFactory()
		for interval == 0 {
			interval = entryIntervalFactory()
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			<-ticker.C
			a, b := rand.Int63n(maxValue), rand.Int63n(maxValue)
			a, b = min(a, b), max(a, b)
			size := 128 * common.Const1MBytes

			entryChan <- newEntry(a, b, size)
			interval = entryIntervalFactory()
			for interval == 0 {
				interval = entryIntervalFactory()
			}
			ticker.Reset(interval)
		}
	}()

	entries := newLockedEntries()
	recentMergedSize := newRecentRecords[int](25)
	go func() {

		ticker := time.NewTicker(mergeInterval)
		defer ticker.Stop()

		var csvWriter *csv.Writer
		if writeToCSV {
			var f *os.File
			var err error
			csvWriter, f, err = createCSVWriter("output.csv")
			if err != nil {
				panic(err)
			}
			defer f.Close()
			defer func() {
				csvWriter.Flush()
				if err = csvWriter.Error(); err != nil {
					panic(err)
				}
			}()

		}

		i := 0
		for {
			select {
			case e := <-entryChan:
				entries.append(e)
			case <-ticker.C:
			}

			if writeToCSV {
				hits := entries.calculateHits(maxValue)
				mean, variance := stat.MeanVariance(hits, nil)
				record := []string{
					strconv.Itoa(i),
					strconv.FormatFloat(mean, 'f', -1, 64),
					strconv.FormatFloat(variance, 'f', -1, 64),
					strconv.FormatFloat(mean/float64(entries.size()), 'f', -1, 64),
					strconv.FormatInt(int64(recentMergedSize.mean()), 10),
				}
				err := csvWriter.Write(record)
				if err != nil {
					panic(err)
				}
				csvWriter.Flush()
			}

			if entries.size() == 0 {
				continue
			}

			i++

			inputss := entries.checkOverlaps()
			for _, inputs := range inputss {
				if len(inputs) < 2 {
					continue
				}
				for _, e := range inputs {
					entries.remove(func(entry *entry) bool {
						return entry.id == e.id
					})
				}
				outputs, size := mergeEntries(inputs, 128*common.Const1MBytes)
				recentMergedSize.add(size / common.Const1MBytes)
				entries.append(outputs...)
			}
		}
	}()

	scanner := bufio.NewScanner(os.Stdin)
	fmt.Printf("input: ")
	for scanner.Scan() {
		switch scanner.Text() {
		case "p":
			entries.RLock()
			entriesList := entries.entries
			segments := make(map[objectio.Segmentid][]*entry)
			for _, e := range entriesList {
				segments[*e.id.Segment()] = append(segments[*e.id.Segment()], e)
			}
			entries.RUnlock()

			segmentSlice := make([]objectio.Segmentid, 0, len(segments))
			for id := range segments {
				segmentSlice = append(segmentSlice, id)
			}

			slices.SortFunc(segmentSlice, func(a, b objectio.Segmentid) int {
				return cmp.Compare(len(segments[a]), len(segments[b]))
			})

			for _, id := range segmentSlice {
				segment := segments[id]
				slices.SortFunc(segment, func(a, b *entry) int {
					if c := a.zm.CompareMin(b.zm); c != 0 {
						return c
					}
					return a.zm.CompareMax(b.zm)
				})

				fmt.Printf("%s(%d): ", id.ShortString(), merge.SegLevel(len(segment)))
				for _, e := range segment {
					fmt.Printf("(%d, %d), ", e.zm.GetMin(), e.zm.GetMax())
				}
				fmt.Printf("\n")
			}

		case "a":
			hits := entries.calculateHits(maxValue)
			fmt.Printf("Ave(hit)=%f, Max(hit)=%f\n", stat.Mean(hits, nil), slices.Max(hits))
			fmt.Printf("Ave(mergedSize)=%f\n", float64(recentMergedSize.mean()))
			fmt.Printf("Max(mergedSize)=%d\n", recentMergedSize.getMax())
		}

		fmt.Printf("input: ")
	}

	if scanner.Err() != nil {
		panic(scanner.Err())
	}

}

func mergeEntries(inputs []*entry, targetSize int) ([]*entry, int) {
	totalSize := 0
	minValue, maxValue := int64(math.MaxInt64), int64(math.MinInt64)
	for _, input := range inputs {
		totalSize += input.size
		if input.zm.GetMin().(int64) < minValue {
			minValue = input.zm.GetMin().(int64)
		}
		if input.zm.GetMax().(int64) > maxValue {
			maxValue = input.zm.GetMax().(int64)
			if maxValue >= 100000 {
				panic("max exceed")
			}
		}
	}

	mergedSize := totalSize
	num := int64(totalSize / targetSize)
	if num == 0 {
		num = 1
	}

	interval := (maxValue - minValue) / num
	entries := make([]*entry, 0)
	segmentID := objectio.NewSegmentid()
	i := uint16(0)
	for {
		entryMax := minValue + interval
		if entryMax > maxValue {
			entryMax = maxValue
		}
		if totalSize < 2*targetSize {
			entries = append(entries, newEntryWithSegmentID(segmentID, i, minValue, entryMax, totalSize))
			i++
			break
		}
		entries = append(entries, newEntryWithSegmentID(segmentID, i, minValue, entryMax, targetSize))
		i++
		minValue += interval
		minValue += 1
		if minValue > maxValue {
			minValue = maxValue
		}
		totalSize -= targetSize
	}

	return entries, mergedSize
}
