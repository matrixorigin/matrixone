// Copyright 2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package merge

import (
	"bufio"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/btree"
)

// region: test utils

func newSortedTestObjectEntry(t testing.TB, v1, v2 int32, size uint32) *catalog.ObjectEntry {
	stats := newTestObjectStats(t, v1, v2, size, 2, 0, nil, 0)
	return &catalog.ObjectEntry{
		ObjectMVCCNode: catalog.ObjectMVCCNode{ObjectStats: *stats},
	}
}

func newTestObjectStats(t testing.TB,
	v1, v2 int32, size, row uint32, lv int8, segid *types.Segmentid, num uint16,
) *objectio.ObjectStats {
	zm := index.NewZM(types.T_int32, 0)
	index.UpdateZM(zm, types.EncodeInt32(&v1))
	index.UpdateZM(zm, types.EncodeInt32(&v2))
	stats := objectio.NewObjectStats()
	var objName objectio.ObjectName
	if segid != nil {
		objName = objectio.BuildObjectName(segid, num)
	} else {
		nobjid := objectio.NewObjectid()
		objName = objectio.BuildObjectNameWithObjectID(&nobjid)
	}
	require.NoError(t, objectio.SetObjectStatsObjectName(stats, objName))
	require.NoError(t, objectio.SetObjectStatsSortKeyZoneMap(stats, zm))
	require.NoError(t, objectio.SetObjectStatsOriginSize(stats, size))
	require.NoError(t, objectio.SetObjectStatsRowCnt(stats, row))
	stats.SetLevel(lv)
	return stats
}

func newTestVarcharObjectEntry(t testing.TB, v1, v2 string, size uint32) *catalog.ObjectEntry {
	zm := index.NewZM(types.T_varchar, 0)
	index.UpdateZM(zm, []byte(v1))
	index.UpdateZM(zm, []byte(v2))
	stats := objectio.NewObjectStats()
	nobjid := objectio.NewObjectid()
	objName := objectio.BuildObjectNameWithObjectID(&nobjid)
	require.NoError(t, objectio.SetObjectStatsObjectName(stats, objName))
	require.NoError(t, objectio.SetObjectStatsSortKeyZoneMap(stats, zm))
	require.NoError(t, objectio.SetObjectStatsOriginSize(stats, size))
	require.NoError(t, objectio.SetObjectStatsRowCnt(stats, 2))
	return &catalog.ObjectEntry{
		ObjectMVCCNode: catalog.ObjectMVCCNode{ObjectStats: *stats},
	}
}

func newTestObjectEntry(t *testing.T, size uint32, isTombstone bool) *catalog.ObjectEntry {
	stats := objectio.NewObjectStats()
	objid := objectio.NewObjectid()
	objName := objectio.BuildObjectNameWithObjectID(&objid)
	require.NoError(t, objectio.SetObjectStatsObjectName(stats, objName))
	require.NoError(t, objectio.SetObjectStatsOriginSize(stats, size))
	require.NoError(t, objectio.SetObjectStatsRowCnt(stats, 2))

	return &catalog.ObjectEntry{
		ObjectMVCCNode: catalog.ObjectMVCCNode{ObjectStats: *stats},
		ObjectNode:     catalog.ObjectNode{IsTombstone: isTombstone},
	}
}

func StatsString(stats objectio.ObjectStats, zonemapKind common.ZonemapPrintKind) string {
	zonemapStr := "nil"
	if z := stats.SortKeyZoneMap(); z != nil {
		switch zonemapKind {
		case common.ZonemapPrintKindNormal:
			zonemapStr = z.String()
		case common.ZonemapPrintKindCompose:
			zonemapStr = z.StringForCompose()
		case common.ZonemapPrintKindHex:
			zonemapStr = z.StringForHex()
		}
	}
	return fmt.Sprintf(
		"%s oSize:%s, cSzie:%s rows:%d, zm: %s",
		stats.ObjectName().ObjectId().ShortStringEx(),
		common.HumanReadableBytes(int(stats.OriginSize())),
		common.HumanReadableBytes(int(stats.Size())),
		stats.Rows(),
		zonemapStr,
	)
}

func DisplayPointEvents(events *btree.BTreeG[*pointEvent]) string {
	iter := events.Iter()
	var sb strings.Builder
	sb.Grow(events.Len() * 50) // Pre-allocate buffer based on estimated size per event

	for iter.Next() {
		event := iter.Item()
		sb.WriteString(event.String())
		sb.WriteByte('\n')
	}
	return sb.String()
}

// func makeStringReader(content string) (io.Reader, func(), error) {
//	return strings.NewReader(content), func() {}, nil
// }

func makeFileReader(filepath string) (io.Reader, func(), error) {
	file, err := os.Open(filepath)
	if err != nil {
		return nil, nil, err
	}
	return file, func() { file.Close() }, nil
}

// parsing the output of `select mo_ctl('dn', 'inspect', 'object -t db.t -vvvv')` into a list of objectio.ObjectStats
func parseObjectFile(reader io.Reader, isTombstone bool) ([]*objectio.ObjectStats, error) {
	var resutls []*objectio.ObjectStats
	scanner := bufio.NewScanner(reader)

	section := 0
	// Section constants for parsing object files
	const (
		SectionNone      = 0
		SectionData      = 1
		SectionTombstone = 2
	)
	targetSection := SectionData
	if isTombstone {
		targetSection = SectionTombstone
	}

	for scanner.Scan() {
		line := scanner.Text()

		// Check if we're entering the DATA section
		if line == "DATA" {
			section = SectionData
			continue
		}

		// Check if we're entering the TOMBSTONES section
		if line == "TOMBSTONES" {
			section = SectionTombstone
			continue
		}

		// Skip lines that are not in the DATA section or are summary lines
		if section != targetSection || strings.HasPrefix(line, "summary:") || strings.TrimSpace(line) == "" {
			continue
		}

		// Parse object entries
		parts := strings.Fields(line)
		if len(parts) >= 2 {
			encodedData := parts[1]

			// Decode base64 data
			decodedData, err := base64.StdEncoding.DecodeString(encodedData)
			if err != nil {
				continue
			}

			// Create ObjectStats from decoded data
			stats := objectio.ObjectStats(decodedData)
			if stats.Rows() > 0 {
				resutls = append(resutls, &stats)
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return resutls, nil
}

// endregion: test utils

// region: overlap test cases

func TestIsConstantObj(t *testing.T) {
	// Test object with max truncated zonemap
	obj := newTestVarcharObjectEntry(t, "test", "test", common.DefaultMinOsizeQualifiedBytes)
	obj.ObjectStats.SortKeyZoneMap().SetMaxTruncated()
	require.True(t, IsConstantObj(obj.GetObjectStats()),
		"Object with max truncated should be constant")

	// Test object smaller than MinOsizeQualifiedMB
	obj = newTestVarcharObjectEntry(t, "same", "same", common.DefaultMinOsizeQualifiedBytes-1)
	require.False(t, IsConstantObj(obj.GetObjectStats()),
		"Small object should not be constant regardless of min/max")

	// Test string object with same min and max values
	obj = newTestVarcharObjectEntry(t, "test", "test", common.DefaultMinOsizeQualifiedBytes)
	require.True(t, IsConstantObj(obj.GetObjectStats()),
		"String object with same min/max should be constant")

	// Test string object with different min and max values
	obj = newTestVarcharObjectEntry(t, "aaa", "zzz", common.DefaultMinOsizeQualifiedBytes)
	require.False(t, IsConstantObj(obj.GetObjectStats()),
		"String object with different min/max should not be constant")

	// Test string object with 30-byte values
	obj = newTestVarcharObjectEntry(t,
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", // 30 'a's
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaab", // 29 'a's + 'b'
		common.DefaultMinOsizeQualifiedBytes)
	require.True(t, IsConstantObj(obj.GetObjectStats()))

	obj = newTestVarcharObjectEntry(t,
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		common.DefaultMinOsizeQualifiedBytes)
	require.True(t, IsConstantObj(obj.GetObjectStats()))

	obj = newTestVarcharObjectEntry(t,
		"\x61\x61\x61\x61\x61\x61\x61\x61\x61\x61\x61\x61\x61\x61\x61\x61\x61\x61\x61\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff",
		"\x61\x61\x61\x61\x61\x61\x61\x61\x61\x61\x61\x61\x61\x61\x61\x61\x61\x61\x62\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
		common.DefaultMinOsizeQualifiedBytes)
	require.True(t, IsConstantObj(obj.GetObjectStats()))

	obj = newTestVarcharObjectEntry(t,
		"\x61\x61\x61\x61\x61\x61\x61\x61\x61\x61\x61\x61\x61\x61\x61\x61\x61\x61\x61\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff",
		"\x61\x61\x61\x61\x61\x61\x61\x61\x61\x61\x61\x61\x61\x61\x61\x61\x61\x61\x62\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00",
		common.DefaultMinOsizeQualifiedBytes)
	require.False(t, IsConstantObj(obj.GetObjectStats()))

	// Test object with uninitialized zonemap
	obj = newTestObjectEntry(t, 0, false)
	require.False(t, IsConstantObj(obj.GetObjectStats()),
		"Object with uninitialized zonemap should not be constant")

	// Test numeric object with same min and max values
	obj = newSortedTestObjectEntry(t, 42, 42, common.DefaultMinOsizeQualifiedBytes)
	require.True(t, IsConstantObj(obj.GetObjectStats()),
		"Numeric object with same min/max should be constant")

	// Test numeric object with different min and max values
	obj = newSortedTestObjectEntry(t, 1, 100, common.DefaultMinOsizeQualifiedBytes)
	require.False(t, IsConstantObj(obj.GetObjectStats()),
		"Numeric object with different min/max should not be constant")
}

func TestOverlapStatsError(t *testing.T) {
	stats := []*objectio.ObjectStats{}
	tasks, err := GatherOverlapMergeTasks(context.Background(), stats, NewOverlapOptions(), 0)
	require.Nil(t, tasks)
	require.NoError(t, err)
	ret, err := CalculateOverlapStats(context.Background(), stats, NewOverlapOptions())
	require.Nil(t, ret)
	require.NoError(t, err)
	stats = append(stats, objectio.NewObjectStats())
	ret, err = CalculateOverlapStats(context.Background(), stats, NewOverlapOptions())
	require.Nil(t, ret)
	require.Error(t, err)
}

func TestOverlapStats(t *testing.T) {
	size := uint32(3 * common.Const1MBytes)
	bigSize := uint32(131 * common.Const1MBytes)

	seg := objectio.NewSegmentid()
	stats := []*objectio.ObjectStats{
		objectio.NewObjectStats(), // uninited
		newTestObjectStats(t, 1, 100, size, 2, 0, seg, 0),
		newTestObjectStats(t, 1, 100, size, 2, 0, seg, 1),
		newTestObjectStats(t, 1, 100, size, 2, 0, seg, 2),
		newTestObjectStats(t, 1, 100, size, 2, 0, seg, 3),
		newTestObjectStats(t, 1, 1, bigSize, 2, 0, seg, 4),

		newTestObjectStats(t, 200, 300, size, 2, 0, seg, 5),
		newTestObjectStats(t, 200, 300, size, 2, 0, seg, 6),
		newTestObjectStats(t, 200, 300, size, 2, 0, seg, 7),
		newTestObjectStats(t, 200, 200, size, 2, 0, seg, 8),
		newTestObjectStats(t, 272, 388, size, 2, 0, seg, 9),
	}

	opts := NewOverlapOptions().WithFitPolynomialDegree(4).WithFurtherStat(true)
	t.Logf("OverlapOpts: %s", opts.String())
	overlapStats, err := CalculateOverlapStats(context.Background(), stats, opts)
	t.Logf("OverlapStats: %s", overlapStats.String())
	iter := overlapStats.PointEvents.Iter()
	for iter.Next() {
		event := iter.Item()
		t.Logf("event: %s", event.String())
	}
	require.NoError(t, err)
	require.Equal(t, overlapStats.ScanObj, 11)
	require.Equal(t, overlapStats.ConstantObj, 1)
	require.Equal(t, overlapStats.UniniteddObj, 1)
	require.Greater(t, overlapStats.AvgPointDepth, 3.0)
	require.Equal(t, 2, len(overlapStats.Clusters))

	mergeTasks, err := GatherOverlapMergeTasks(context.Background(), stats, opts, 1)
	require.NoError(t, err)
	require.Equal(t, 2, len(mergeTasks))
	for _, task := range mergeTasks {
		require.Equal(t, int8(1), task.level)
		require.Equal(t, 4, len(task.objs))
	}

	// bump min point depth per cluster to 5 to make no merge tasks
	opts = opts.WithMinPointDepthPerCluster(5)
	mergeTasks, err = GatherOverlapMergeTasks(context.Background(), stats, opts, 0)
	require.NoError(t, err)
	require.Equal(t, 0, len(mergeTasks))
}

func TestCalculateOverlapStats(t *testing.T) {
	t.Skip("used to processing file")
	// Path to the zout file
	zoutFilePath := "/root/matrixone/zmtest/statement2.2-2.out"

	reader, closer, err := makeFileReader(zoutFilePath)
	if err != nil {
		t.Fatalf("Failed to make file reader: %v", err)
	}
	defer closer()

	statsList, err := parseObjectFile(reader, false)
	if err != nil {
		t.Fatalf("Failed to parse zout file: %v", err)
	}

	// Log the extracted entries
	t.Logf("Extracted %d entries from zout file", len(statsList))
	leveledObjects := [8][]*objectio.ObjectStats{}
	for _, stat := range statsList {
		lv := stat.GetLevel()
		leveledObjects[lv] = append(leveledObjects[lv], stat)
	}

	for i, objects := range leveledObjects {
		if len(objects) < 2 {
			continue
		}
		t.Logf("level %d: %v", i, len(objects))
		// Calculate overlap metrics
		opts := NewOverlapOptions().WithFitPolynomialDegree(4).WithFurtherStat(true)
		overlapStats, err := CalculateOverlapStats(context.Background(), objects, opts)
		require.NoError(t, err)

		mergeTasks, err := GatherOverlapMergeTasks(context.Background(), objects, opts, int8(i))
		require.NoError(t, err)
		for _, task := range mergeTasks {
			t.Logf("merge tasks found: %v", task.String())
		}
		t.Logf("OverlapStats: %s", overlapStats.String())
	}
}

// endregion: overlap test cases

// region: zero layer test cases

func TestL0Stats(t *testing.T) {
	size := uint32(20 * common.Const1MBytes)
	stats := []*objectio.ObjectStats{
		newTestObjectStats(t, 1, 100, size, 2, 1, nil, 0),
		newTestObjectStats(t, 1, 100, size, 2, 0, nil, 1),
		newTestObjectStats(t, 1, 100, size, 2, 0, nil, 2),
		newTestObjectStats(t, 1, 100, size, 2, 0, nil, 2),
		newTestObjectStats(t, 1, 100, size, 2, 0, nil, 2),
		newTestObjectStats(t, 1, 100, size, 2, 0, nil, 2),
	}

	ctx := context.Background()
	opts := NewLayerZeroOpts()
	t.Logf("LayerZeroOpts: %s", opts.String())

	l0stats := CalculateLayerZeroStats(ctx, stats, 0, opts)
	t.Logf("LayerZeroStats: %s", l0stats.String())
	require.Equal(t, 5, l0stats.Count)

	l0stats = CalculateLayerZeroStats(ctx, stats, 1*time.Hour, opts)
	require.Less(t, l0stats.Tolerance, l0stats.Count)

	l0stats = CalculateLayerZeroStats(ctx, stats, 2*time.Hour, opts.Clone().WithLowerBoundSize(30*common.Const1MBytes))
	require.Zero(t, l0stats.Count)

	mergeTasks := GatherLayerZeroMergeTasks(ctx, stats, 10*time.Minute, opts)
	require.Equal(t, 0, len(mergeTasks))

	mergeTasks = GatherLayerZeroMergeTasks(ctx, nil, 10*time.Minute, opts)
	require.Equal(t, 0, len(mergeTasks))

	mergeTasks = GatherLayerZeroMergeTasks(ctx, stats, 1*time.Hour, opts)
	require.Equal(t, 1, len(mergeTasks))
	require.Equal(t, int8(0), mergeTasks[0].level)
	require.Equal(t, 5, len(mergeTasks[0].objs))

	stats = append(stats, newTestObjectStats(t, 1, 100, size*2, 2, 0, nil, 2))
	mergeTasks = GatherLayerZeroMergeTasks(ctx, stats, 0, opts)
	require.Equal(t, 1, len(mergeTasks))
	require.Equal(t, 6, len(mergeTasks[0].objs))

}

func TestLayerZeroOpts_WithToleranceDegressionCurve(t *testing.T) {
	opts := NewLayerZeroOpts()

	// Test setting new values
	newStart := 200
	newEnd := 10
	newDuration := 2 * time.Hour
	newCPoints := [4]float64{0.5, 0.2, 0.8, 1.0}

	opts = opts.WithToleranceDegressionCurve(newStart, newEnd, newDuration, newCPoints)

	require.Equal(t, newStart, opts.Start, "Start should match new value")
	require.Equal(t, newEnd, opts.End, "End should match new value")
	require.Equal(t, newDuration, opts.Duration, "Duration should match new value")
	require.Equal(t, newCPoints, opts.CPoints, "CPoints should match new values")

	lowerSize := uint32(1024)
	upperSize := uint32(2048)
	lowerRows := uint32(100)
	upperRows := uint32(200)

	opts = opts.WithLowerBoundSize(lowerSize).
		WithUpperBoundSize(upperSize).
		WithLowerBoundRows(lowerRows).
		WithUpperBoundRows(upperRows)

	require.Equal(t, lowerSize, opts.LowerSelectSize, "LowerSelectSize should match")
	require.Equal(t, upperSize, opts.UpperSelectSize, "UpperSelectSize should match")
	require.Equal(t, lowerRows, opts.LowerSelectRows, "LowerSelectRows should match")
	require.Equal(t, upperRows, opts.UpperSelectRows, "UpperSelectRows should match")
}

func TestLayerZeroOpts_CalcTolerance(t *testing.T) {
	opts := NewLayerZeroOpts()

	// Test no merge yet
	tolerance := opts.CalcTolerance(0)
	require.Equal(t, opts.Start, tolerance, "When no merge yet, tolerance should equal Start value")

	// Test exceeded duration
	tolerance = opts.CalcTolerance(2 * time.Hour)
	require.Equal(t, opts.End, tolerance, "When exceeded duration, tolerance should equal End value")

	// Test half duration
	tolerance = opts.CalcTolerance(30 * time.Minute)
	require.Greater(t, tolerance, opts.End, "At half duration, tolerance should be greater than End")
	require.Less(t, tolerance, opts.Start, "At half duration, tolerance should be less than Start")

	tolerance = opts.CalcTolerance(2 * TenYears)
	require.GreaterOrEqual(t, tolerance, opts.End, "When duration is OVER 20 years, tolerance should be greater than End")
	require.LessOrEqual(t, tolerance, opts.Start, "When duration is OVER 20 years, tolerance should be less than Start")
}

func TestSizeLevel(t *testing.T) {
	// Test with 0 bytes
	result := sizeLevel(0, 7)
	require.Equal(t, 0, result, "0 bytes should return level 0")

	// Test size 512KB (less than 1MB)
	result = sizeLevel(512*1024, 7)
	require.Equal(t, 0, result, "sizeLevel should return 0 for size 512KB")

	// Test size exactly 1MB
	result = sizeLevel(1*1024*1024, 7)
	require.Equal(t, 1, result, "sizeLevel should return 1 for size 1MB")

	// Test size 1.5MB (between 1MB and 2MB)
	result = sizeLevel(1536*1024, 7)
	require.Equal(t, 1, result, "sizeLevel should return 1 for size 1.5MB")

	// Test size exactly 2MB
	result = sizeLevel(2*1024*1024, 7)
	require.Equal(t, 2, result, "sizeLevel should return 2 for size 2MB")

	// Test size 4MB
	result = sizeLevel(4*1024*1024, 7)
	require.Equal(t, 3, result, "sizeLevel should return 3 for size 4MB")

	// Test size 8MB
	result = sizeLevel(8*1024*1024, 7)
	require.Equal(t, 4, result, "sizeLevel should return 4 for size 8MB")

	// Test size 16MB
	result = sizeLevel(16*1024*1024, 7)
	require.Equal(t, 5, result, "sizeLevel should return 5 for size 16MB")

	// Test size 32MB
	result = sizeLevel(32*1024*1024, 7)
	require.Equal(t, 6, result, "sizeLevel should return 6 for size 32MB")

	// Test size 64MB
	result = sizeLevel(64*1024*1024, 7)
	require.Equal(t, 7, result, "sizeLevel should return 7 for size 64MB")

	// Test size 128MB (exceeds max level)
	result = sizeLevel(128*1024*1024, 7)
	require.Equal(t, 7, result, "sizeLevel should return 7 for size 128MB (capped by maxLevel)")

	// Test with lower maxLevel (3)
	result = sizeLevel(16*1024*1024, 3)
	require.Equal(t, 3, result, "sizeLevel should return 3 for size 16MB when maxLevel is 3")
}

func TestShowCurve(t *testing.T) {
	t.Skip("used to update comments")
	opts := NewLayerZeroOpts()
	s := ""
	v := ""
	for i := 1; i < 61; i++ {
		s += fmt.Sprintf("%d\t", i)
		v += fmt.Sprintf("%d\t", opts.CalcTolerance(time.Duration(i)*time.Minute))
		if i%20 == 0 {
			fmt.Printf("// time:\t%s\n", s)
			fmt.Printf("// val :\t%s\n", v)
			s = ""
			v = ""
			fmt.Println("//")
		}
	}
}

func TestTimeLevelSince(t *testing.T) {
	// Test less than 1 minute
	result := timeLevelSince(30 * time.Second)
	require.Equal(t, 0, result, "30 seconds should return level 0")

	// Test exactly 1 minute
	result = timeLevelSince(1 * time.Minute)
	require.Equal(t, 1, result, "1 minute should return level 1")

	// Test between 1 and 10 minutes
	result = timeLevelSince(5 * time.Minute)
	require.Equal(t, 1, result, "5 minutes should return level 1")

	// Test exactly 10 minutes
	result = timeLevelSince(10 * time.Minute)
	require.Equal(t, 2, result, "10 minutes should return level 2")

	// Test between 10 and 30 minutes
	result = timeLevelSince(20 * time.Minute)
	require.Equal(t, 2, result, "20 minutes should return level 2")

	// Test exactly 30 minutes
	result = timeLevelSince(30 * time.Minute)
	require.Equal(t, 3, result, "30 minutes should return level 3")

	// Test greater than 30 minutes
	result = timeLevelSince(1 * time.Hour)
	require.Equal(t, 3, result, "1 hour should return level 3")

	// Test very large duration
	result = timeLevelSince(24 * time.Hour)
	require.Equal(t, 3, result, "24 hours should return level 3")
}

// endregion: zero layer test cases

// region: vacuum test cases

func TestTombstoneOpts(t *testing.T) {

	opts := NewTombstoneOpts()
	t.Logf("TombstoneOpts: %s", opts.String())
	small := newTestObjectStats(t, 1, 100, 1024*1024, 2, 0, nil, 0)
	big := newTestObjectStats(t, 1, 100, 16*1024*1024, 2, 0, nil, 4)
	stats := []*objectio.ObjectStats{}
	for i := 0; i < opts.L1Count-1; i++ {
		stats = append(stats, small)
	}
	for i := 0; i < opts.L2Count-1; i++ {
		stats = append(stats, big)
	}
	ctx := context.Background()

	tasks := GatherTombstoneTasks(ctx, IterStats(stats), opts, 0)
	require.Equal(t, 0, len(tasks))

	tasks = GatherTombstoneTasks(ctx, IterStats(stats), opts.Clone().WithOneShot(true), 0)
	require.Equal(t, 1, len(tasks))
	require.Contains(t, tasks[0].note, "oneshot")
	tasks = GatherTombstoneTasks(ctx, IterStats(stats), opts.Clone().WithOneShot(true), 2*time.Hour)
	require.Equal(t, 1, len(tasks))

	stats = append(stats, small, big)
	tasks = GatherTombstoneTasks(ctx, IterStats(stats), opts, 0)
	require.Equal(t, 2, len(tasks))
	require.Contains(t, tasks[0].note, "small")
	require.Contains(t, tasks[1].note, "big")

	tasks = GatherTombstoneTasks(ctx, IterStats(stats), opts.Clone().WithL2Count(10), 0)
	require.Equal(t, 1, len(tasks))
	require.Contains(t, tasks[0].note, "small")

	tasks = GatherTombstoneTasks(ctx,
		IterStats(stats),
		opts.Clone().WithL1(4*1024*1024, 10).WithL2Count(10),
		0,
	)
	require.Equal(t, 0, len(tasks))
}

func TestVacuumOpts(t *testing.T) {

	fs := objectio.TmpNewFileservice(context.Background(), "/tmp/vacuum-test")
	rt := dbutils.NewRuntime(
		dbutils.WithRuntimeObjectFS(fs),
		dbutils.WithRuntimeLocalFS(fs),
		dbutils.WithRuntimeSmallPool(dbutils.MakeDefaultSmallPool("small-vector-pool")),
		dbutils.WithRuntimeTransientPool(dbutils.MakeDefaultTransientPool("trasient-vector-pool")),
	)

	dataFactory := tables.NewDataFactory(
		rt, "/tmp/data-test",
	)
	defer func() {
		os.RemoveAll("/tmp/vacuum-test")
	}()

	schema := catalog.MockSchema(3, 1)
	schema.Name = "test"
	batch := catalog.MockBatch(schema, 8192*4)
	bats := batch.Split(4)
	names := []objectio.ObjectName{
		objectio.BuildObjectName(objectio.NewSegmentid(), 0),
		objectio.BuildObjectName(objectio.NewSegmentid(), 0),
		objectio.BuildObjectName(objectio.NewSegmentid(), 0),
		objectio.BuildObjectName(objectio.NewSegmentid(), 0),
	}
	dataStats := []*objectio.ObjectStats{}
	tombStats := []*objectio.ObjectStats{}
	for i, name := range names {
		writer, err := ioutil.NewBlockWriterNew(fs, name, 1, nil, false)
		require.NoError(t, err)
		_, err = writer.WriteBatch(containers.ToCNBatch(bats[i]))
		require.NoError(t, err)
		blocks, _, err := writer.Sync(context.Background())
		require.NoError(t, err)
		require.Equal(t, 1, len(blocks))
		stat := writer.GetObjectStats()
		dataStats = append(dataStats, &stat)

		if i > 2 {
			break // only 3 objects have tombstone
		}

		bat := containers.NewBatch()
		bat.AddVector(
			objectio.TombstoneAttrs_CN_Created[0],
			containers.NewVector(types.T_Rowid.ToType()),
		)
		bat.AddVector(
			objectio.TombstoneAttrs_CN_Created[1],
			containers.NewVector(types.T_int32.ToType()),
		)

		for j := 2000 * i; j < 2000*(i+1); j++ {
			bat.Vecs[0].Append(
				types.NewRowIDWithObjectIDBlkNumAndRowID(*name.ObjectId(), 0, uint32(j)),
				false,
			)
			bat.Vecs[1].Append(
				int32(j),
				false,
			)
		}

		tombwriter, err := ioutil.NewBlockWriterNew(
			fs,
			objectio.BuildObjectName(objectio.NewSegmentid(), 0),
			1,
			nil,
			true,
		)
		require.NoError(t, err)
		tombwriter.WriteBatch(containers.ToCNBatch(bat))
		blocks, _, err = tombwriter.Sync(context.Background())
		require.NoError(t, err)
		require.Equal(t, 1, len(blocks))
		tombStat := tombwriter.GetObjectStats()
		tombStats = append(tombStats, &tombStat)
	}

	{
		dataStats[1].SetLevel(2)
	}

	cata := catalog.MockCatalog(dataFactory)
	db := catalog.MockDBEntryWithAccInfo(1000, 1000)
	db.TestSetCatalog(cata)
	tbl := catalog.NewTableEntryWithTableId(
		db,
		schema,
		nil,
		dataFactory.MakeTableFactory(),
		1001,
	)
	for _, stat := range dataStats {
		entry := catalog.MockObjectEntry(
			tbl,
			stat,
			false,
			dataFactory.MakeObjectFactory(),
			types.BuildTS(time.Now().UnixNano(), 0),
		)
		tbl.AddEntryLocked(entry)
	}
	for _, stat := range tombStats {
		entry := catalog.MockObjectEntry(
			tbl,
			stat,
			true,
			dataFactory.MakeObjectFactory(),
			types.BuildTS(time.Now().UnixNano(), 0),
		)
		tbl.AddEntryLocked(entry)
	}

	//// test
	ctx := context.Background()
	opts := NewVacuumOpts()
	t.Logf("VacuumOpts: %s", opts.String())

	mTable := catalog.ToMergeTable(tbl)
	stats, err := CalculateVacuumStats(ctx, mTable, opts.Clone().WithEnableDetail(false), time.Now())
	require.NoError(t, err)
	mergeTasks := GatherCompactTasks(ctx, stats)
	require.Equal(t, 0, len(mergeTasks))

	stats, err = CalculateVacuumStats(ctx, mTable, opts.Clone().WithCheckBigOnly(false), time.Now())
	require.NoError(t, err)
	t.Logf("stats: %s", stats.String())

	{
		opts = opts.Clone().WithCheckBigOnly(false).
			WithStartScore(1).
			WithEndScore(1).
			WithDuration(1 * time.Minute).
			WithHollowTopK(2)
		stats, err = CalculateVacuumStats(ctx, mTable, opts, time.Now())
		require.NoError(t, err)
		mergeTasks := GatherCompactTasks(ctx, stats)
		require.Equal(t, 2, len(mergeTasks))
	}
}

// endregion: vacuum test cases
