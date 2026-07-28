// Copyright 2021 Matrix Origin
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

package interactive

import (
	"fmt"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
)

// Helper functions for formatting

func formatTS(ts types.TS) string {
	physical := ts.Physical()
	if physical == 0 {
		return fmt.Sprintf("%d-%d", physical, ts.Logical())
	}
	t := time.Unix(0, physical)
	return fmt.Sprintf("%d-%d(%s)", physical, ts.Logical(), t.Format("2006/01/02 15:04:05.000000"))
}

func formatTSShort(ts types.TS) string {
	if ts.IsEmpty() {
		return "-"
	}
	t := ts.Physical()
	return time.Unix(0, t).Format("01-02 15:04")
}

func stateStr(s checkpoint.State) string {
	switch s {
	case checkpoint.ST_Running:
		return "Running"
	case checkpoint.ST_Pending:
		return "Pending"
	case checkpoint.ST_Finished:
		return "Finished"
	default:
		return "Unknown"
	}
}

func formatSize(bytes uint32) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%dB", bytes)
	}
	div, exp := uint32(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

func parseSize(s string) uint32 {
	var val float64
	var unit string
	fmt.Sscanf(s, "%f%s", &val, &unit)
	switch unit {
	case "KB":
		return uint32(val * 1024)
	case "MB":
		return uint32(val * 1024 * 1024)
	case "GB":
		return uint32(val * 1024 * 1024 * 1024)
	case "B":
		return uint32(val)
	default:
		return uint32(val)
	}
}

func expandObjectStats(value any) []any {
	data, ok := value.([]byte)
	if !ok || len(data) != objectio.ObjectStatsLen {
		return []any{"", "", "", "", ""}
	}

	var stats objectio.ObjectStats
	stats.UnMarshal(data)

	flags := ""
	if stats.GetAppendable() {
		flags += "A"
	}
	if stats.GetSorted() {
		flags += "S"
	}
	if stats.GetCNCreated() {
		flags += "C"
	}
	if flags == "" {
		flags = "-"
	}

	return []any{
		stats.ObjectName().String(),
		flags,
		stats.Rows(),
		formatSize(stats.OriginSize()),
		formatSize(stats.Size()),
	}
}

func ckpDataOverview(rows [][]string) string {
	if len(rows) == 0 {
		return "No data"
	}

	type objStats struct {
		rows  uint32
		osize uint32
		csize uint32
	}
	dataObjs := make(map[string]*objStats)
	tombObjs := make(map[string]*objStats)

	for _, row := range rows {
		if len(row) < 9 {
			continue
		}
		objType := row[3]
		objName := row[4]
		rowsStr := row[6]
		osize := parseSize(row[7])
		csize := parseSize(row[8])

		var rowCount uint32
		fmt.Sscanf(rowsStr, "%d", &rowCount)

		if objType == "1" {
			if _, exists := dataObjs[objName]; !exists {
				dataObjs[objName] = &objStats{rows: rowCount, osize: osize, csize: csize}
			}
		} else if objType == "2" {
			if _, exists := tombObjs[objName]; !exists {
				tombObjs[objName] = &objStats{rows: rowCount, osize: osize, csize: csize}
			}
		}
	}

	var totalRows, totalDeletes, totalOsize, totalCsize uint32
	for _, s := range dataObjs {
		totalRows += s.rows
		totalOsize += s.osize
		totalCsize += s.csize
	}
	for _, s := range tombObjs {
		totalDeletes += s.rows
	}

	ratio := float64(0)
	if totalOsize > 0 {
		ratio = float64(totalCsize) / float64(totalOsize) * 100
	}

	return fmt.Sprintf("%d ranges │ %d data objs │ %d tomb objs │ %d rows │ %d deletes │ osize: %s │ csize: %s │ ratio: %.1f%%",
		len(rows), len(dataObjs), len(tombObjs), totalRows, totalDeletes,
		formatSize(totalOsize), formatSize(totalCsize), ratio)
}
