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

package ioutil

import "github.com/matrixorigin/matrixone/pkg/container/types"

type TSRangeFile struct {
	start types.TS
	end   types.TS
	name  string
	idx   int
	ext   string
}

func (m TSRangeFile) IsCKPFile() bool {
	return IsCKPExt(m.ext)
}

func (m *TSRangeFile) GetStart() *types.TS {
	return &m.start
}

func (m *TSRangeFile) GetEnd() *types.TS {
	return &m.end
}

func (m *TSRangeFile) GetExt() string {
	return m.ext
}

func (m *TSRangeFile) GetName() string {
	return m.name
}

func (m *TSRangeFile) GetFullName() string {
	return GetCheckpointDir() + m.name
}

func (m *TSRangeFile) GetIdx() int {
	return m.idx
}

func (m *TSRangeFile) SetIdx(idx int) {
	m.idx = idx
}
