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

	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/btree"
)

// region: test utils
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

func makeObjectEntry(stats *objectio.ObjectStats, isTombstone bool) *catalog.ObjectEntry {
	return &catalog.ObjectEntry{
		ObjectMVCCNode: catalog.ObjectMVCCNode{ObjectStats: *stats},
		ObjectNode:     catalog.ObjectNode{IsTombstone: isTombstone},
	}
}

func makeFileReader(filepath string) (io.Reader, func(), error) {
	file, err := os.Open(filepath)
	if err != nil {
		return nil, nil, err
	}
	return file, func() { file.Close() }, nil
}

func makeStringReader(content string) (io.Reader, func(), error) {
	return strings.NewReader(content), func() {}, nil
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

func TestXxx(t *testing.T) {
	objs := []*catalog.ObjectEntry{}
	objs = append(objs, newTestObjectEntry(t, 1, false))
	objs = append(objs, newTestObjectEntry(t, 2, false))
	objs = append(objs, newTestObjectEntry(t, 3, false))
	objs = append(objs, newTestObjectEntry(t, 4, false))
	objs = append(objs, newTestObjectEntry(t, 5, false))
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

func TestCalculateOverlapMetrics(t *testing.T) {
	// Path to the zout file
	zoutFilePath := "/root/matrixone/zmtest/dev-statement.out"

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
	leveledObjects := [][]*objectio.ObjectStats{statsList}

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

	// selected := []*objectio.ObjectStats{}
	// sysPrefix, _ := hex.DecodeString("460173797300")
	// for _, stats := range statsList {
	// 	if stats.SortKeyZoneMap().PrefixEq(sysPrefix) {
	// 		selected = append(selected, stats)
	// 	}
	// }
	// {
	// 	policy := newOldObjOverlapPolicy()
	// 	policy.resetForTable(nil, defaultBasicConfig)
	// 	rc := new(resourceController)
	// 	rc.setMemLimit(30 * common.Const1GBytes)
	// 	for _, stats := range statsList {
	// 		policy.onObject(makeObjectEntry(stats, false))
	// 	}
	// 	results := policy.revise(rc)
	// 	for _, result := range results {
	// 		t.Logf("revise result: %v", result.String())
	// 	}
	// }
}

// endregion: overlap test cases

// region: zero layer test cases

func TestCalculateLayerZeroStats(t *testing.T) {
	// Path to the zout file
	// zoutFilePath := "/root/matrixone/zmtest/old-statement.out"
	zoutFilePath := "/root/matrixone/zmtest/dev-rawlog-3.out"

	reader, closer, err := makeFileReader(zoutFilePath)
	if err != nil {
		t.Fatalf("Failed to make file reader: %v", err)
	}
	defer closer()

	statsList, err := parseObjectFile(reader, false)
	if err != nil {
		t.Fatalf("Failed to parse zout file: %v", err)
	}
	opts := NewLayerZeroOpts()

	stats := CalculateLayerZeroStats(context.Background(), statsList, 0, opts)
	t.Logf("LayerZeroStats: %s", stats.String())
	tasks, err := GatherLayerZeroMergeTasks(context.Background(), statsList, 0, opts)
	require.NoError(t, err)
	for _, task := range tasks {
		t.Logf("task: %s", task.String())
		for _, obj := range task.objs[:10] {
			t.Logf("obj: %s", StatsString(*obj, common.ZonemapPrintKindNormal))
		}
	}
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
	require.Greater(t, tolerance, opts.End, "When duration is OVER 20 years, tolerance should be greater than End")
	require.Less(t, tolerance, opts.Start, "When duration is OVER 20 years, tolerance should be less than Start")
}

func TestShowCurve(t *testing.T) {
	// t.Skip("used to update comments")
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

// endregion: zero layer test cases
