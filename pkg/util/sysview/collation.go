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

package sysview

import (
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/collation"
)

// CollationDefinition is the canonical metadata for a collation identity that
// MatrixOne can execute or expose through compatibility metadata. SHOW
// COLLATION exposes the definitions with Advertised set, while the same
// definitions populate the information-schema metadata surfaces.
type CollationDefinition struct {
	Name         string
	Charset      string
	ID           int64
	IsDefault    string
	IsCompiled   string
	SortLen      int32
	PadAttribute string
	Advertised   bool
}

// SupportedCollationDefinitions is a presentation of the common capability
// owner, not a second registry. Native-but-disabled domains are not advertised.
var SupportedCollationDefinitions = func() []CollationDefinition {
	var result []CollationDefinition
	for _, d := range collation.Advertised() {
		row := CollationDefinition{Name: d.Name, Charset: d.Charset.Name(), ID: int64(d.ProtocolID),
			IsCompiled: "Yes", SortLen: d.SortLen, PadAttribute: "NO PAD", Advertised: true}
		if d.Default {
			row.IsDefault = "YES"
		}
		if d.PadSpace {
			row.PadAttribute = "PAD SPACE"
		}
		result = append(result, row)
	}
	return result
}()

// DefaultCollationForCharset returns the canonical default collation exposed
// by the information_schema and SHOW COLLATION compatibility surfaces.
// An empty result means that the canonical definitions do not contain a
// default for the requested character set.
func DefaultCollationForCharset(charset string) string {
	for _, definition := range SupportedCollationDefinitions {
		if strings.EqualFold(definition.Charset, collation.CanonicalCharsetName(charset)) && definition.IsDefault == "YES" {
			return definition.Name
		}
	}
	return ""
}

// InformationSchemaCharacterSetsCheckSQL is shared by tenant upgrades so their
// completion predicate uses the same effective capacities as fresh catalogs.
// The exact row count also detects duplicate or obsolete advertised charsets.
func InformationSchemaCharacterSetsCheckSQL() string {
	charsets := []string{"binary", "utf8", "utf8mb4"}
	clauses := []string{fmt.Sprintf("(SELECT COUNT(*) FROM information_schema.CHARACTER_SETS) = %d", len(charsets))}
	for _, charset := range charsets {
		clauses = append(clauses, fmt.Sprintf(
			"EXISTS (SELECT 1 FROM information_schema.CHARACTER_SETS "+
				"WHERE CHARACTER_SET_NAME = '%s' AND DEFAULT_COLLATE_NAME = '%s' AND MAXLEN = %d)",
			charset, DefaultCollationForCharset(charset), characterSetMaxBytes(charset)))
	}
	return "SELECT 1 WHERE " + strings.Join(clauses, " AND ")
}

// Report the admitted encoding's capacity, not the strict native domain's.
// utf8/utf8mb3 are four-byte UTF-8 compatibility spellings in current SQL.
func characterSetMaxBytes(charset string) int32 {
	identity, ok := collation.ResolveCharset(charset)
	if !ok {
		return 0
	}
	d, _ := collation.EffectiveDefinition(uint32(identity), 0)
	return d.Charset.MaxBytes()
}
