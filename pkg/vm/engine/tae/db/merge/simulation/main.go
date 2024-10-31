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
	"math"
	"math/rand"
	"os"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spf13/cobra"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/compute"
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
	MergeSimulationCmd.Flags().Int64("max", 10000, "set max value of all ranges")
	MergeSimulationCmd.Flags().StringP("new_entry_distribution", "d", "M", "distribution of entry arrival in Kendall's notation")
	MergeSimulationCmd.Flags().Float64P("new_entry_distribution_arg", "a", 20, "arguments for distribution")
	MergeSimulationCmd.Flags().DurationP("merge_interval", "i", 50*time.Millisecond, "set merge interval in ms")
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

func (l *lockedEntries) getEntries() []*entry {
	l.RLock()
	defer l.RUnlock()
	return l.entries
}

func (l *lockedEntries) append(e ...*entry) {
	l.Lock()
	defer l.Unlock()
	l.entries = append(l.entries, e...)
}

func (l *lockedEntries) calculateHits(r int64) []float64 {
	l.RLock()
	defer l.RUnlock()

	hits := make([]float64, 0, r)
	for i := range r {
		hit := 0
		for _, e := range l.entries {
			zmMax := e.zm.GetMax().(int64)
			zmMin := e.zm.GetMin().(int64)
			if zmMin <= i && i < zmMax {
				hit++
			}
		}
		hits = append(hits, float64(hit))
	}

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

var levels = [6]int{
	1, 2, 4, 16, 64, 256,
}

func segLevel(length int) int {
	l := 5
	for i, level := range levels {
		if length < level {
			l = i - 1
			break
		}
	}
	return l
}

func (l *lockedEntries) segments() [6][]*entry {
	entries := l.getEntries()
	segments := make(map[objectio.Segmentid][]*entry)
	for _, e := range entries {
		segments[*e.id.Segment()] = append(segments[*e.id.Segment()], e)
	}

	leveledObjects := [6][]*entry{}

	for _, segment := range segments {
		level := segLevel(len(segment))
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

func (l *lockedEntries) checkOverlaps() [6][]*entry {

	leveledEntries := l.segments()
	leveledOutputs := [6][]*entry{}

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
		if len(objs) > 16 {
			objs = objs[:16]
		}
		leveledOutputs[i] = append(leveledOutputs[i], objs...)
	}
	return leveledOutputs
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
	var totalMergedSize atomic.Int64
	var i atomic.Int32
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
					strconv.Itoa(int(i.Load())),
					strconv.FormatFloat(mean, 'f', -1, 64),
					strconv.FormatFloat(variance, 'f', -1, 64),
					strconv.FormatFloat(mean/float64(entries.size()), 'f', -1, 64),
					strconv.FormatFloat(float64(totalMergedSize.Load())/float64(i.Load()), 'f', -1, 64),
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

			i.Add(1)

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
				outputs, mergedSize := merge(inputs, 128*common.Const1MBytes)
				totalMergedSize.Add(int64(mergedSize) / common.Const1MBytes)
				entries.append(outputs...)
			}
		}
	}()

	scanner := bufio.NewScanner(os.Stdin)
	fmt.Printf("input: ")
	for scanner.Scan() {
		switch scanner.Text() {
		case "p":
			entriesList := entries.getEntries()
			segments := make(map[objectio.Segmentid][]*entry)
			for _, e := range entriesList {
				segments[*e.id.Segment()] = append(segments[*e.id.Segment()], e)
			}

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

				fmt.Printf("%s(%d): ", id.ShortString(), segLevel(len(segment)))
				for _, e := range segment {
					fmt.Printf("(%d, %d), ", e.zm.GetMin(), e.zm.GetMax())
				}
				fmt.Printf("\n")
			}

		case "a":
			hits := entries.calculateHits(maxValue)
			mean, variance := stat.MeanVariance(hits, nil)
			fmt.Printf("Ave(hit)=%f, Var(hit)=%f\n", mean, variance)
			fmt.Printf("Ave(hit/n)=%f\n", mean/float64(entries.size()))
			fmt.Printf("Ave(mergedSize)=%f\n", float64(totalMergedSize.Load())/float64(i.Load()))
		}

		fmt.Printf("input: ")
	}

	if scanner.Err() != nil {
		panic(scanner.Err())
	}

}

func merge(inputs []*entry, targetSize int) ([]*entry, int) {
	totalSize := 0
	minValue, maxValue := int64(math.MaxInt64), int64(math.MinInt64)
	for _, input := range inputs {
		totalSize += input.size
		if input.zm.GetMin().(int64) < minValue {
			minValue = input.zm.GetMin().(int64)
		}
		if input.zm.GetMax().(int64) > maxValue {
			maxValue = input.zm.GetMax().(int64)
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
			entries = append(entries, newEntryWithSegmentID(segmentID, i, minValue, minValue+interval, totalSize))
			i++
			break
		}
		entries = append(entries, newEntryWithSegmentID(segmentID, i, minValue, minValue+interval, targetSize))
		i++
		minValue += interval
		minValue += 1
		totalSize -= targetSize
	}

	return entries, mergedSize
}
