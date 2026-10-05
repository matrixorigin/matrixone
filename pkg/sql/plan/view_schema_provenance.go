// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import "encoding/json"

// ViewSchemaColumnProvenance describes one completed View output. Source is
// an independent metadata snapshot. QueryBuilder-local relation/column slots
// deliberately do not escape this boundary: a consumer must bind its own slots.
type ViewSchemaColumnProvenance struct {
	State             ProvenanceState
	CTASDefaultPolicy CTASDefaultPolicy
	SourceTableID     uint64
	Source            *SourceColumnMetadata
}

type viewSchemaOrigin struct {
	State     ProvenanceState
	Policy    CTASDefaultPolicy
	TableID   uint64
	HasSource bool
}
type viewSchemaOrigins struct {
	Origins []viewSchemaOrigin
	Sources []byte
}

func encodeViewSchemaProvenance(origins []OutputColumnProvenance, columns []*ColDef) ([]byte, error) {
	value := viewSchemaOrigins{Origins: make([]viewSchemaOrigin, len(columns))}
	sources := &TableDef{Cols: make([]*ColDef, len(columns))}
	for i, column := range columns {
		sources.Cols[i] = &ColDef{}
		if i >= len(origins) {
			continue
		}
		origin := origins[i]
		value.Origins[i] = viewSchemaOrigin{State: origin.State, Policy: origin.CTASDefaultPolicy}
		if origin.Source != nil {
			metadata := origin.Source.Metadata
			metadata.NullAbility = !column.Typ.NotNullable
			value.Origins[i].HasSource = true
			value.Origins[i].TableID = origin.Source.TableID
			value.Origins[i].Policy = ctasViewDefaultPolicy(metadata)
			def := DeepCopyDefault(metadata.Default)
			// The explicit-default presence is carried separately by the metadata
			// source: an absent default must stay absent for CTAS policy decisions.
			sources.Cols[i] = &ColDef{Typ: metadata.Typ, Default: def, NotNull: !metadata.NullAbility}
		}
	}
	var err error
	value.Sources, err = sources.Marshal()
	if err != nil {
		return nil, err
	}
	return json.Marshal(value)
}

func (r *ViewSchemaResult) Provenance() ([]ViewSchemaColumnProvenance, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.request == nil {
		return nil, ErrViewSchemaClosed
	}
	var value viewSchemaOrigins
	if err := json.Unmarshal(r.provenance, &value); err != nil {
		return nil, err
	}
	var sources TableDef
	if err := sources.Unmarshal(value.Sources); err != nil {
		return nil, err
	}
	result := make([]ViewSchemaColumnProvenance, len(value.Origins))
	for i, origin := range value.Origins {
		result[i] = ViewSchemaColumnProvenance{State: origin.State, CTASDefaultPolicy: origin.Policy, SourceTableID: origin.TableID}
		if origin.HasSource {
			source := sources.Cols[i]
			result[i].Source = &SourceColumnMetadata{Typ: source.Typ, Default: source.Default, NullAbility: !source.NotNull}
		}
	}
	return result, nil
}

func (r *ViewSchemaResult) RequiredProtocolVersion() (int64, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.request == nil {
		return 0, ErrViewSchemaClosed
	}
	return r.requiredProtocol, nil
}
