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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
)

// Helper functions for formatting

func formatTS(ts types.TS) string {
	raw := ts.ToString()
	physical := ts.Physical()
	if physical == 0 {
		return raw
	}
	t := time.Unix(0, physical)
	return fmt.Sprintf("%s(%s)", raw, t.Format("2006/01/02 15:04:05.000000"))
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
