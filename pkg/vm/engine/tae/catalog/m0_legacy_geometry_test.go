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

package catalog

import (
	"github.com/matrixorigin/matrixone/pkg/internal/m0read"
	"github.com/matrixorigin/matrixone/pkg/objectio"
)

// These legacy geometric examples are controlled component fixtures only.
// They are not authority constructors and cannot be linked into a product.
// M0DerivedAppendVersion recovers the schema version represented by a complete
// append-only schema. It is for the CN/TN writers that materialize the full
// current batch after an in-memory or catalog version marker has gone stale.
func M0DerivedAppendVersion(s *Schema) (uint32, error) {
	layout, err := M0DerivedAppendLayout(s)
	if err != nil {
		return 0, err
	}
	return layout.Version, nil
}

// M0DerivedAppendLayout reconstructs the append-domain layout from complete
// column identities. The local catalog Version is intentionally not used:
// relation creation can restore all columns without restoring the inherited
// append version, while background merge must still qualify read-only objects.
func M0DerivedAppendLayout(s *Schema) (m0read.M0AppendLayout, error) {
	layout, err := M0AppendLayoutFromSchema(s)
	if err != nil {
		return m0read.M0AppendLayout{}, err
	}
	base := uint32(layout.PKSeqnum) + 1
	if layout.NextColSeqnum <= base {
		return m0read.M0AppendLayout{}, wrapM0Error(ErrM0Layout, "schema has no append version")
	}
	if s.Extra.M0Evolution == nil {
		layout.Version = layout.NextColSeqnum - base
	}
	if err = m0read.ValidateM0AppendLayout(layout); err != nil {
		return m0read.M0AppendLayout{}, wrapM0Error(ErrM0Layout, "%v", err)
	}
	return layout, nil
}

// M0AppendBase validates one deliberately narrow transition domain:
//   - version-zero CREATE/COPY puts the hidden composite PK last;
//   - each independent autocommit ADD appends exactly one nullable column and
//     advances the version once;
//   - clone preserves the full layout AND version; other INPLACE DDL is refused;
//   - COPY starts a new version-zero domain and rewrites all data objects.
//
// The mo_m0 SQL/CN/TN hooks enforce these transitions. This predicate remains
// scoped to the restricted prototype and is not authorization for arbitrary
// legacy tables.
func M0AppendBase(s *Schema) (uint32, error) {
	if err := validateM0Schema(s); err != nil {
		return 0, err
	}
	layout, err := M0AppendLayoutFromSchema(s)
	if err != nil {
		return 0, err
	}
	if err = m0read.ValidateM0AppendLayout(layout); err != nil {
		return 0, wrapM0Error(ErrM0Layout, "%v", err)
	}
	return uint32(layout.PKSeqnum) + 1, nil
}

// M0ProveAbsentColumn returns true only for a nullable suffix that did not exist
// at the object's version in the above domain. It validates every block's
// complete expected prefix, rather than accepting MaxSeqnum alone. Metadata
// must come from the standard object reader. An object must already be a member
// of the relation being read; this does not invent cross-relation ownership.
// There is no historical-schema lookup, NULL vector allocation or pruning hook.
func M0ProveAbsentColumn(s *Schema, h objectio.Header, meta objectio.ObjectDataMeta, seq uint16) (bool, error) {
	if _, err := M0AppendBase(s); err != nil {
		return false, err
	}
	layout, err := M0AppendLayoutFromSchema(s)
	if err != nil {
		return false, err
	}
	absent, err := m0read.ProveM0AbsentColumn(layout, h, meta, seq)
	if err != nil {
		return false, wrapM0Error(ErrM0Layout, "%v", err)
	}
	return absent, nil
}
