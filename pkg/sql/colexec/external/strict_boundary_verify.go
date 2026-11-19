// Copyright 2026 Matrix Origin
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

package external

import (
	"bufio"
	"context"
	"io"
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/util/csvparser"
	"github.com/spkg/bom"
)

// One attempt owns both sequential streams; neither allowance is reset.
type strictProbeAttempt struct {
	remaining int64
	calls     int
}

func (a *strictProbeAttempt) nextRecord() error {
	if a.calls >= strictProbeMaxRecords {
		return errStrictProbeBudgetExceeded
	}
	a.calls++
	return nil
}

var errStrictBoundaryUnverified = moerr.NewInternalError(moerr.NoReportContext(), "strict CSV candidate boundary is unverified")

// strictBoundaryPrefix captures only the three bytes used by bom.Clean.
// offset and target are relative to FileStartOff, not parser.Pos().
type strictBoundaryPrefix struct {
	io.Reader
	target int64
	offset int64
	prefix [3]byte
	n      int
}

func (r *strictBoundaryPrefix) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	start := r.target + int64(r.n) - r.offset
	if start >= 0 && start < int64(n) {
		r.n += copy(r.prefix[r.n:], p[int(start):n])
	}
	r.offset += int64(n)
	return n, err
}

// verifyStrictTarget proves exactly target from the execution's initial start.
// It does not bless any earlier suffix heuristic as a restart anchor.
// The caller owns open/close and passes only its remaining attempt budget.
func verifyStrictTarget(param *tree.ExternParam, cfg *csvparser.CSVConfig, r io.Reader, target int64, attempt *strictProbeAttempt) error {
	ctx := param.Ctx
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if target <= 0 || target > math.MaxInt64-3 || target > attempt.remaining {
		return errStrictProbeBudgetExceeded
	}
	limited := &strictProbeReader{Reader: r, remaining: attempt.remaining}
	defer func() { attempt.remaining = limited.remaining }()
	prefix := &strictBoundaryPrefix{Reader: limited, target: target}
	contextual := boundaryContextReader{ctx: ctx, Reader: prefix}
	buffer := bufio.NewReaderSize(contextual, strictProbeBufferSize)
	parser, err := csvparser.NewCSVParser(cfg, buffer, strictProbeBlockSize, false)
	if err != nil {
		return err
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := attempt.nextRecord(); err != nil {
			return err
		}
		err := parser.ScanRecord()
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if err != nil {
			return err
		}
		if parser.Pos() > target {
			return errStrictBoundaryUnverified
		}
		if parser.Pos() != target {
			continue
		}
		if prefix.n < len(prefix.prefix) {
			// Physical prefetch did not include the entire restart prefix.
			// Read only the missing bytes from the same stream and budget.
			var scratch [3]byte
			if _, err := io.ReadFull(contextual, scratch[:len(scratch)-prefix.n]); err != nil {
				return err
			}
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if len(bom.Clean(prefix.prefix[:])) != len(prefix.prefix) {
			return errStrictBoundaryUnverified
		}
		return nil
	}
}
