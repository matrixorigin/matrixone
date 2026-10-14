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

// Package ttl owns versioned table expiration metadata and safe SQL generation.
package ttl

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

const PropertyKey = "mo_ttl"

// Fixed-length intervals only. A finite bound keeps arithmetic within time.Duration.
const MaxDurationSeconds uint64 = 100 * 365 * 24 * 60 * 60

type Policy struct {
	Version int    `json:"version"`
	Column  string `json:"column"`
	Seqnum  uint32 `json:"seqnum"`
	Seconds uint64 `json:"seconds"`
}

func IsUserDatabase(database string) bool {
	switch strings.ToLower(database) {
	case "mo_catalog", "information_schema", "mysql", "system", "system_metrics", "mo_task", "mo_debug":
		return false
	default:
		return true
	}
}

func Duration(amount uint64, unit string) (uint64, error) {
	var multiplier uint64
	switch strings.ToUpper(unit) {
	case "SECOND":
		multiplier = 1
	case "MINUTE":
		multiplier = 60
	case "HOUR":
		multiplier = 3600
	case "DAY":
		multiplier = 86400
	case "WEEK":
		multiplier = 604800
	default:
		return 0, moerr.NewInvalidInputNoCtxf("TTL supports SECOND, MINUTE, HOUR, DAY and WEEK intervals")
	}
	if amount == 0 || amount > MaxDurationSeconds/multiplier {
		return 0, moerr.NewInvalidInputNoCtxf("TTL interval must be positive and at most 100 years")
	}
	return amount * multiplier, nil
}

func (p Policy) Validate() error {
	if p.Version != 1 || p.Column == "" || len(p.Column) > maxDiscoveryNameBytes || p.Seqnum > 65535 || p.Seconds == 0 || p.Seconds > MaxDurationSeconds {
		return moerr.NewInvalidInputNoCtxf("invalid TTL policy")
	}
	return nil
}
func Decode(s string) (*Policy, error) {
	var p Policy
	decoder := json.NewDecoder(strings.NewReader(s))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&p); err != nil {
		return nil, moerr.NewInvalidInputNoCtxf("invalid TTL metadata: %v", err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return nil, moerr.NewInvalidInputNoCtxf("invalid trailing TTL metadata")
	}
	if err := p.Validate(); err != nil {
		return nil, err
	}
	return &p, nil
}
func Encode(p *Policy) string {
	b, err := json.Marshal(p)
	if err != nil {
		panic(err)
	}
	return string(b)
}
func FromProperties(properties []*plan.Property) (*Policy, error) {
	var result *Policy
	for _, prop := range properties {
		if prop.Key != PropertyKey {
			continue
		}
		if result != nil {
			return nil, moerr.NewInvalidInputNoCtxf("TTL specified more than once")
		}
		p, err := Decode(prop.Value)
		if err != nil {
			return nil, err
		}
		result = p
	}
	return result, nil
}
func Get(def *plan.TableDef) (*Policy, error) {
	var properties []*plan.Property
	for _, d := range def.Defs {
		if p := d.GetProperties(); p != nil {
			properties = append(properties, p.Properties...)
		}
	}
	return FromProperties(properties)
}
func Set(def *plan.TableDef, p *Policy) {
	for _, d := range def.Defs {
		if props := d.GetProperties(); props != nil {
			kept := props.Properties[:0]
			for _, prop := range props.Properties {
				if prop.Key != PropertyKey {
					kept = append(kept, prop)
				}
			}
			props.Properties = kept
		}
	}
	if p != nil {
		def.Defs = append(def.Defs, &plan.TableDef_DefType{Def: &plan.TableDef_DefType_Properties{Properties: &plan.PropertiesDef{Properties: []*plan.Property{{Key: PropertyKey, Value: Encode(p)}}}}})
	}
}
func ValidateColumn(def *plan.TableDef, p *Policy) error {
	if err := p.Validate(); err != nil {
		return err
	}
	for _, col := range def.Cols {
		if col.Name != p.Column {
			continue
		}
		if col.Seqnum != p.Seqnum || col.Hidden {
			break
		}
		switch types.T(col.Typ.Id) {
		case types.T_date, types.T_datetime, types.T_timestamp:
			return nil
		}
		break
	}
	return moerr.NewInvalidInputNoCtxf("TTL requires an existing DATE, DATETIME or TIMESTAMP column with matching identity")
}
func Clause(p *Policy) string {
	return fmt.Sprintf("TTL %s + INTERVAL %d SECOND DELETE", sqlquote.Ident(p.Column), p.Seconds)
}

// DeleteSQL uses a fixed UTC cutoff, avoiding overflow at the upper end of a
// temporal column. NULL compares UNKNOWN and is retained. SQL DELETE preserves
// indexes, MVCC snapshots and constraints through the normal transaction path.
func DeleteSQL(database, table string, p *Policy, now time.Time, batchSize int) (string, error) {
	if err := p.Validate(); err != nil {
		return "", err
	}
	if batchSize <= 0 {
		return "", moerr.NewInvalidInputNoCtxf("TTL batch size must be positive")
	}
	cutoff := now.UTC().Add(-time.Duration(p.Seconds) * time.Second)
	if cutoff.Year() < 1 || cutoff.Year() > 9999 {
		return "", moerr.NewInvalidInputNoCtxf("TTL cutoff outside supported temporal range")
	}
	return fmt.Sprintf("DELETE FROM %s WHERE %s <= CAST('%s' AS DATETIME(6)) LIMIT %d", sqlquote.QualifiedIdent(database, table), sqlquote.Ident(p.Column), cutoff.Format("2006-01-02 15:04:05.000000"), batchSize), nil
}
