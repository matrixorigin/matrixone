//go:build mo_m0

// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package objectio

import (
	"errors"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

var ErrM0AppendLayout = errors.New("M0: unsupported append layout")

type M0AppendColumn struct {
	Seqnum        uint16
	Type          types.T
	Nullable      bool
	Hidden        bool
	Primary       bool
	SortKey       bool
	ClusterBy     bool
	AutoIncrement bool
	Generated     bool
	OnUpdate      bool
	Backfill      bool
}

type M0AppendLayout struct {
	Version       uint32
	NextColSeqnum uint32
	PKSeqnum      uint16
	CompositePK   bool
	HiddenPK      bool
	SingleSortKey bool
	Columns       []M0AppendColumn
}

func (l M0AppendLayout) base() uint32 {
	return uint32(l.PKSeqnum) + 1
}

func ValidateM0AppendLayout(layout M0AppendLayout) error {
	if !layout.CompositePK || !layout.HiddenPK || !layout.SingleSortKey {
		return fmt.Errorf("%w: hidden composite primary-key anchor", ErrM0AppendLayout)
	}
	if layout.NextColSeqnum == 0 || layout.NextColSeqnum != uint32(len(layout.Columns)) ||
		uint32(layout.PKSeqnum) >= layout.NextColSeqnum {
		return fmt.Errorf("%w: dense ordinary column sequence", ErrM0AppendLayout)
	}
	base := layout.base()
	if layout.Version != layout.NextColSeqnum-base {
		return fmt.Errorf("%w: schema version is outside the append-only domain", ErrM0AppendLayout)
	}
	for i, col := range layout.Columns {
		if col.Seqnum != uint16(i) {
			return fmt.Errorf("%w: column %d has sequence %d", ErrM0AppendLayout, i, col.Seqnum)
		}
		switch {
		case i == int(layout.PKSeqnum):
			if !col.Hidden || !col.Primary || !col.SortKey {
				return fmt.Errorf("%w: primary-key identity at %d", ErrM0AppendLayout, i)
			}
		case i < int(base):
			if col.Hidden || col.Primary || col.SortKey {
				return fmt.Errorf("%w: base column identity at %d", ErrM0AppendLayout, i)
			}
		default:
			if !col.Nullable || col.Hidden || col.Primary || col.SortKey || col.ClusterBy ||
				col.AutoIncrement || col.Generated || col.OnUpdate || col.Backfill {
				return fmt.Errorf("%w: appended column %d requires unsupported semantics", ErrM0AppendLayout, i)
			}
		}
	}
	return nil
}

func ValidateM0AppendTransition(before, after M0AppendLayout) error {
	if err := ValidateM0AppendLayout(before); err != nil {
		return err
	}
	if err := ValidateM0AppendLayout(after); err != nil {
		return err
	}
	if after.Version != before.Version+1 ||
		after.NextColSeqnum != before.NextColSeqnum+1 ||
		after.PKSeqnum != before.PKSeqnum ||
		len(after.Columns) != len(before.Columns)+1 {
		return fmt.Errorf("%w: transition does not append exactly one version", ErrM0AppendLayout)
	}
	for i := range before.Columns {
		if before.Columns[i] != after.Columns[i] {
			return fmt.Errorf("%w: existing column identity changed at %d", ErrM0AppendLayout, i)
		}
	}
	added := after.Columns[len(after.Columns)-1]
	if added.Seqnum != uint16(before.NextColSeqnum) || !added.Nullable ||
		added.Hidden || added.Primary || added.SortKey || added.ClusterBy ||
		added.AutoIncrement || added.Generated || added.OnUpdate || added.Backfill {
		return fmt.Errorf("%w: appended column semantics", ErrM0AppendLayout)
	}
	return nil
}

// ValidateM0Object validates the complete physical prefix promised by the
// object's own schema version. An absent suffix is legal; a hole in the
// expected prefix is never interpreted as NULL.
func ValidateM0Object(layout M0AppendLayout, h Header, meta ObjectDataMeta) (uint32, error) {
	if err := ValidateM0AppendLayout(layout); err != nil {
		return 0, err
	}
	version, err := M0ObjectSchemaVersion(h)
	if err != nil {
		return 0, err
	}
	if version > layout.Version || meta.IsEmpty() || meta.BlockCount() == 0 {
		return 0, fmt.Errorf("%w: object is outside the schema version domain", ErrM0AppendLayout)
	}
	width := layout.base() + version
	if width > layout.NextColSeqnum {
		return 0, fmt.Errorf("%w: object width exceeds the schema", ErrM0AppendLayout)
	}
	if uint32(meta.BlockHeader().MaxSeqnum())+1 != width {
		return 0, fmt.Errorf(
			"%w: object maxseq=%d width=%d declared-version=%d base=%d",
			ErrM0AppendLayout,
			meta.BlockHeader().MaxSeqnum(),
			uint32(meta.BlockHeader().MaxSeqnum())+1,
			version,
			layout.base(),
		)
	}
	for blockIdx := uint32(0); blockIdx < meta.BlockCount(); blockIdx++ {
		block := meta.GetBlockMeta(blockIdx)
		if uint32(block.GetMaxSeqnum())+1 != width || uint32(block.GetColumnCount()) < width {
			return 0, fmt.Errorf("%w: block width does not match the declared object version", ErrM0AppendLayout)
		}
		for seq := uint32(0); seq < width; seq++ {
			col := block.MustGetColumn(uint16(seq))
			if col.DataType() == 0 || col.DataType() != uint8(layout.Columns[seq].Type) {
				return 0, fmt.Errorf("%w: missing or mismatched expected column %d", ErrM0AppendLayout, seq)
			}
		}
	}
	return width, nil
}

func ProveM0AbsentColumn(
	layout M0AppendLayout,
	h Header,
	meta ObjectDataMeta,
	seqnum uint16,
) (bool, error) {
	if uint32(seqnum) >= layout.NextColSeqnum {
		return false, fmt.Errorf("%w: column is outside the read schema", ErrM0AppendLayout)
	}
	width, err := ValidateM0Object(layout, h, meta)
	if err != nil {
		return false, err
	}
	if uint32(seqnum) < width {
		return false, nil
	}
	return true, nil
}
