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

// SupportedCollationDefinitions is the single source of truth for the
// supported collation mapping. The information_schema.COLLATIONS table and
// COLLATION_CHARACTER_SET_APPLICABILITY view are populated from every entry;
// the protocol SHOW COLLATION implementation uses the same advertised set.
var SupportedCollationDefinitions = []CollationDefinition{
	{Name: "utf8_general_ci", Charset: "utf8", ID: 33, IsDefault: "YES", IsCompiled: "Yes", SortLen: 1, PadAttribute: "PAD SPACE", Advertised: true},
	{Name: "binary", Charset: "binary", ID: 63, IsDefault: "YES", IsCompiled: "Yes", SortLen: 1, PadAttribute: "NO PAD", Advertised: true},
	{Name: "utf8_bin", Charset: "utf8", ID: 83, IsDefault: "", IsCompiled: "Yes", SortLen: 1, PadAttribute: "PAD SPACE", Advertised: true},
	{Name: "utf8mb4_general_ci", Charset: "utf8mb4", ID: 45, IsDefault: "YES", IsCompiled: "Yes", SortLen: 1, PadAttribute: "PAD SPACE", Advertised: true},
	{Name: "utf8mb4_bin", Charset: "utf8mb4", ID: 46, IsDefault: "", IsCompiled: "Yes", SortLen: 1, PadAttribute: "PAD SPACE", Advertised: true},
	{Name: "utf8mb4_0900_ai_ci", Charset: "utf8mb4", ID: 255, IsDefault: "", IsCompiled: "Yes", SortLen: 1, PadAttribute: "PAD SPACE", Advertised: true},
}
