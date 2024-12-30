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

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

type TSRangeFile struct {
	start types.TS
	end   types.TS
	name  string
	idx   int
	ext   string
}

func NewTSRangeFile(name, ext string, start, end types.TS) TSRangeFile {
	return TSRangeFile{
		start: start,
		end:   end,
		name:  name,
		ext:   ext,
	}
}

func (m *TSRangeFile) SetInvalid() {
	m.ext = InvalidExt
}

func (m TSRangeFile) IsValid() bool {
	return m.ext != InvalidExt
}

func (m TSRangeFile) IsMetadataFile() bool {
	return IsMetadataName(m.name)
}

func (m TSRangeFile) IsCompactExt() bool {
	return m.ext == CompactExt
}

func (m TSRangeFile) IsCKPFile() bool {
	return IsCKPExt(m.ext)
}

func (m TSRangeFile) IsSnapshotExt() bool {
	return m.ext == SnapshotExt
}

func (m TSRangeFile) IsFullGCExt() bool {
	return m.ext == GCFullExt
}

func (m TSRangeFile) IsAcctExt() bool {
	return m.ext == AcctExt
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

func (m *TSRangeFile) GetCKPFullName() string {
	return MakeFullName(GetCheckpointDir(), m.name)
}

func (m *TSRangeFile) GetGCFullName() string {
	return MakeFullName(GetGCDir(), m.name)
}

func (m *TSRangeFile) GetIdx() int {
	return m.idx
}

func (m *TSRangeFile) SetIdx(idx int) {
	m.idx = idx
}

func (m *TSRangeFile) SetExt(ext string) {
	m.ext = ext
}

func (m *TSRangeFile) RangeEqual(start, end *types.TS) bool {
	return m.start.EQ(start) && m.end.EQ(end)
}

func (m *TSRangeFile) AsString(prefix string) string {
	if !m.IsValid() {
		return "[Invalid]"
	}
	return fmt.Sprintf(
		"%s[%s|%s|%s|%s]",
		prefix,
		m.name,
		m.ext,
		m.start.ToString(),
		m.end.ToString(),
	)
}
