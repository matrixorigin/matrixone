// Copyright 2021-2024 Matrix Origin
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

package readutil

import (
	"github.com/stretchr/testify/require"
	"testing"
)

// Add for just pass UT coverage check
func TestEmptyRelationData(t *testing.T) {
	relData := BuildEmptyRelData()
	require.True(t, len(relData.String()) > 0)
	require.Panics(t, func() {
		relData.GetShardIDList()
	})
	require.Panics(t, func() {
		relData.GetShardID(0)
	})
	require.Panics(t, func() {
		relData.SetShardID(0, 0)
	})
	require.Panics(t, func() {
		relData.AppendShardID(0)
	})
	require.Panics(t, func() {
		relData.GetBlockInfoSlice()
	})
	require.Panics(t, func() {
		relData.GetBlockInfo(0)
	})
	require.Panics(t, func() {
		relData.SetBlockInfo(0, nil)
	})
	require.Panics(t, func() {
		relData.AppendBlockInfo(nil)
	})
	require.Panics(t, func() {
		relData.AppendBlockInfoSlice(nil)
	})
	require.Panics(t, func() {
		relData.DataSlice(0, 0)
	})

	require.Equal(t, 0, relData.DataCnt())
}

//func TestQueryMetadataScan(t *testing.T) {
//	dsn := "dump:111@tcp(127.0.0.1:6001)/test"
//
//	db, err := sql.Open("mysql", dsn)
//	if err != nil {
//		t.Fatalf("failed to connect to database: %v", err)
//	}
//	defer db.Close()
//
//	query := `SELECT object_name, rows_cnt, origin_size, min, max FROM metadata_scan("system.statement_info", "request_at") g;`
//
//	s := time.Now()
//	rows, err := db.Query(query)
//	if err != nil {
//		t.Fatalf("query execution failed: %v", err)
//	}
//	defer rows.Close()
//
//	fmt.Println("query takes", time.Since(s))
//
//	var (
//		objectName []string
//		mmin, mmax []types.Datetime
//		rowsCnt    []int64
//		originSize []int64
//	)
//
//	for rows.Next() {
//		var (
//			a    string
//			b, c int64
//			d, e []byte
//		)
//
//		err = rows.Scan(&a, &b, &c, &d, &e)
//		if err != nil {
//			t.Fatalf("failed to scan row: %v", err)
//		}
//
//		objectName = append(objectName, a)
//		rowsCnt = append(rowsCnt, b)
//		originSize = append(originSize, int64(c))
//		mmin = append(mmin, types.DecodeDatetime(d))
//		mmax = append(mmax, types.DecodeDatetime(e))
//	}
//
//	// 2025-01-20 00:00:00
//	// 2025-01-20 00:00:01
//	t1 := types.DatetimeFromClock(2025, 01, 20, 00, 00, 00, 00)
//	t2 := types.DatetimeFromClock(2025, 01, 20, 00, 00, 01, 00)
//
//	var interval []TimeInterval
//
//	objCnt := 0
//	for i := range mmin {
//		if !(mmin[i] > t2 || t1 > mmax[i]) {
//			objCnt++
//			interval = append(interval, TimeInterval{
//				Start: mmin[i].ConvertToGoTime(time.UTC),
//				End:   mmax[i].ConvertToGoTime(time.UTC),
//			})
//		}
//	}
//
//	plotIntervals(interval)
//	fmt.Println(objCnt, len(objectName))
//}
//
//type TimeInterval struct {
//	Start time.Time
//	End   time.Time
//}
//
//func plotIntervals(intervals []TimeInterval) error {
//	p := plot.New()
//	p.Title.Text = "Time Intervals Visualization"
//	p.X.Label.Text = "Time"
//	p.Y.Label.Text = "Interval Index"
//
//	p.X.Tick.Marker = plot.TimeTicks{Format: "15:04"}
//
//	minTime := intervals[0].Start
//	maxTime := intervals[0].End
//	for _, interval := range intervals {
//		if interval.Start.Before(minTime) {
//			minTime = interval.Start
//		}
//		if interval.End.After(maxTime) {
//			maxTime = interval.End
//		}
//	}
//	p.X.Min = float64(minTime.Unix())
//	p.X.Max = float64(maxTime.Unix())
//
//	for i, interval := range intervals {
//		line, _ := plotter.NewLine(plotter.XYs{
//			{X: float64(interval.Start.Unix()), Y: float64(i)},
//			{X: float64(interval.End.Unix()), Y: float64(i)},
//		})
//		line.LineStyle.Width = vg.Points(2)
//		p.Add(line)
//	}
//
//	p.Y.Min = -1
//	p.Y.Max = float64(len(intervals))
//
//	if err := p.Save(10*vg.Inch, 4*vg.Inch, "time_intervals.pn"); err != nil {
//		panic(err)
//	}
//	return nil
//}
