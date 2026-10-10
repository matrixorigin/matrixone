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
// owner, not a second registry. Every native domain that is executable is
// advertised with its own protocol identity; legacy aliases use their effective
// admitted semantics.
var SupportedCollationDefinitions = func() []CollationDefinition {
	advertised := collation.Advertised()
	result := make([]CollationDefinition, 0, len(advertised))
	for _, d := range advertised {
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
	clauses := make([]string, 0, 1+len(charsets))
	clauses = append(clauses, fmt.Sprintf("(SELECT COUNT(*) FROM information_schema.CHARACTER_SETS) = %d", len(charsets)))
	for _, charset := range charsets {
		clauses = append(clauses, fmt.Sprintf(
			"EXISTS (SELECT 1 FROM information_schema.CHARACTER_SETS "+
				"WHERE CHARACTER_SET_NAME = '%s' AND DEFAULT_COLLATE_NAME = '%s' AND MAXLEN = %d)",
			charset, DefaultCollationForCharset(charset), characterSetMaxBytes(charset)))
	}
	return "SELECT 1 WHERE " + strings.Join(clauses, " AND ")
}

// databaseDefaultsMetadataSQL projects admitted effective identities, not SQL
// aliases. Unknown/corrupt metadata returns NULL rather than a server fallback.
func databaseDefaultsMetadataSQL(charset bool) string {
	var sql strings.Builder
	sql.WriteString("case")
	for _, definition := range collation.Definitions() {
		for revision := uint32(0); revision <= uint32(collation.RevisionV1); revision++ {
			identity := uint32(definition.Identity)
			if err := collation.RequireLegacy(identity, revision, collation.KeyFormatLegacy); err != nil {
				continue
			}
			effective, err := collation.EffectiveDefinition(identity, revision)
			if err != nil {
				continue
			}
			name := effective.Name
			if charset {
				name = effective.Charset.Name()
			}
			fmt.Fprintf(&sql, " when dd.collation_id = %d and dd.collation_revision = %d then '%s'", identity, revision, name)
		}
	}
	sql.WriteString(" else NULL end")
	return sql.String()
}

// The SQL internal_column_character_set classifier uses presentation codes
// 4/5 for the already-admitted native domains. Names/capacity still come from
// the shared semantic owner; this projection does not admit a new domain.
func informationSchemaNativeColumnsDDL() string {
	return strings.NewReplacer(
		"(case internal_column_character_set(mc.atttyp) WHEN 0 then 'utf8' WHEN 1 then 'utf8mb4' WHEN 2 then 'binary' WHEN 3 then 'utf8mb4' else NULL end)",
		columnDomainMetadataSQL("mc.atttyp", true),
		"(case internal_column_character_set(mc.atttyp) WHEN 0 then 'utf8_general_ci' WHEN 1 then 'utf8mb4_bin' WHEN 2 then 'binary' WHEN 3 then 'utf8mb4_general_ci' else NULL end)",
		ColumnCollationSQL("mc.atttyp"),
	).Replace(InformationSchemaColumnsDDL)
}

// ColumnCollationSQL shares the effective-domain projection between COLUMNS
// and SHOW FULL COLUMNS. The expression is a planner-owned encoded type.
func ColumnCollationSQL(encodedType string) string {
	return columnDomainMetadataSQL(encodedType, false)
}

func columnDomainMetadataSQL(encodedType string, charset bool) string {
	var sql strings.Builder
	fmt.Fprintf(&sql, "(case internal_column_character_set(%s)", encodedType)
	// These are classifier presentation codes, not persisted identities.
	for code, identity := range []collation.Identity{collation.UTF8MB4GeneralCIIdentity, collation.UTF8MB4BinIdentity,
		collation.BinaryIdentity, collation.UTF8MB4GeneralCIIdentity, collation.UTF8UnicodeCIIdentity, collation.UTF8MB4UnicodeCIIdentity} {
		revision := uint32(collation.RevisionLegacy)
		if code >= 4 {
			revision = uint32(collation.RevisionV1)
		}
		if err := collation.RequireLegacy(uint32(identity), revision, collation.KeyFormatLegacy); err != nil {
			continue
		}
		definition, err := collation.EffectiveDefinition(uint32(identity), revision)
		if err != nil {
			continue
		}
		if code == 0 {
			// This classifier code is absent historical column metadata, not
			// the persisted modern default. Keep its released spelling.
			definition, _ = collation.Lookup("utf8_general_ci")
		}
		name := definition.Name
		if charset {
			name = definition.Charset.Name()
		}
		fmt.Fprintf(&sql, " WHEN %d then '%s'", code, name)
	}
	sql.WriteString(" else NULL end)")
	return sql.String()
}

// Legacy databases have no persisted pair. Project the effective server
// domain, including compatibility aliases, instead of advertising the spelling.
func databaseServerMetadataSQL(charset bool) string {
	var sql strings.Builder
	sql.WriteString("case lower(@@session.collation_server)")
	for _, definition := range collation.Advertised() {
		identity, revision := uint32(definition.LegacyIdentity), uint32(0)
		if definition.Semantics == collation.UCA400 {
			identity, revision = uint32(definition.Identity), uint32(collation.RevisionV1)
		}
		effective, err := collation.EffectiveDefinition(identity, revision)
		if err != nil {
			continue
		}
		name := effective.Name
		if charset {
			name = effective.Charset.Name()
		}
		fmt.Fprintf(&sql, " when '%s' then '%s'", definition.Name, name)
	}
	sql.WriteString(" else NULL end")
	return sql.String()
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
