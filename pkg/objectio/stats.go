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

package objectio

import (
	"bytes"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/util/metric/stats"
)

type selectivityStats struct {
	hit   stats.Counter
	total stats.Counter
}

func (s *selectivityStats) Record(hit, total int) {
	s.total.Add(int64(total))
	s.hit.Add(int64(hit))
}

func (s *selectivityStats) Export() (hit, total int64) {
	hit = s.hit.Load()
	total = s.total.Load()
	return
}

func (s *selectivityStats) ExportW() (hit, total int64) {
	hit = s.hit.SwapW(0)
	total = s.total.SwapW(0)
	return
}

func (s *selectivityStats) ExportAll() (whit, wtotal int64, hit, total int64) {
	whit = s.hit.SwapW(0)
	wtotal = s.total.SwapW(0)
	hit = s.hit.Swap(0)
	total = s.total.Swap(0)
	return
}

type Stats struct {
	blockSelectivity      selectivityStats
	columnSelectivity     selectivityStats
	readFilterSelectivity selectivityStats
}

func NewStats() *Stats {
	return &Stats{}
}

func (s *Stats) RecordReadFilterSelectivity(hit, total int) {
	s.readFilterSelectivity.Record(hit, total)
}

func (s *Stats) ExportReadFilterSelectivity() (
	whit, wtotal int64, hit, total int64,
) {
	whit, wtotal = s.readFilterSelectivity.ExportW()
	if wtotal == 0 {
		whit = 0
	}
	hit, total = s.readFilterSelectivity.Export()
	return
}

func (s *Stats) RecordBlockSelectivity(hit, total int) {
	s.blockSelectivity.Record(hit, total)
}

func (s *Stats) ExportBlockSelectivity() (
	whit, wtotal int64,
) {
	whit, wtotal, _, _ = s.blockSelectivity.ExportAll()
	if wtotal == 0 {
		whit = 0
	}
	return
}

func (s *Stats) RecordColumnSelectivity(hit, total int) {
	s.columnSelectivity.Record(hit, total)
}

func (s *Stats) ExportColumnSelctivity() (
	hit, total int64,
) {
	hit, total, _, _ = s.columnSelectivity.ExportAll()
	if total == 0 {
		hit = 0
	}
	return
}

func (s *Stats) ExportString() string {
	var w bytes.Buffer
	whit, wtotal := s.ExportBlockSelectivity()
	wrate, rate := 0.0, 0.0
	if wtotal != 0 {
		wrate = float64(whit) / float64(wtotal)
	}
	fmt.Fprintf(&w, "SelectivityStats: BLK[%d/%d=%0.2f] ", whit, wtotal, wrate)
	whit, wtotal = s.ExportColumnSelctivity()
	wrate, rate = 0.0, 0.0
	if wtotal != 0 {
		wrate = float64(whit) / float64(wtotal)
	}
	fmt.Fprintf(&w, "COL[%d/%d=%0.2f] ", whit, wtotal, wrate)
	whit, wtotal, hit, total := s.ExportReadFilterSelectivity()
	wrate, rate = 0.0, 0.0
	if wtotal != 0 {
		wrate = float64(whit) / float64(wtotal)
	}
	if total != 0 {
		rate = float64(hit) / float64(total)
	}
	fmt.Fprintf(&w, "RDF[%d/%d=%0.2f,%d/%d=%0.2f]", whit, wtotal, wrate, hit, total, rate)
	return w.String()
}
