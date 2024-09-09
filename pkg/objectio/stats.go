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
	"github.com/matrixorigin/matrixone/pkg/util/metric/stats"
)

type hitStats struct {
	hit   stats.Counter
	total stats.Counter
}

func (s *hitStats) Record(hit, total int) {
	s.total.Add(int64(total))
	s.hit.Add(int64(hit))
}

func (s *hitStats) Export() (hit, total int64) {
	hit = s.hit.Load()
	total = s.total.Load()
	return
}

func (s *hitStats) ExportW() (hit, total int64) {
	hit = s.hit.SwapW(0)
	total = s.total.SwapW(0)
	return
}
