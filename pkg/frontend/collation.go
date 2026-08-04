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

package frontend

type Collation struct {
	collationName string
	charset       string
	id            int64
	isDefault     string
	isCompiled    string
	sortLen       int32
	padAttribute  string
}

// Collations is the executable capability list exposed by SHOW COLLATION.
// Keep unsupported UCA/0900 names out of this list: advertising them as
// compiled while DDL rejects them makes capability discovery unusable.
var Collations []*Collation = []*Collation{
	{"utf8_general_ci", "utf8", 33, "YES", "Yes", 1, "PAD SPACE"},
	{"binary", "binary", 63, "YES", "Yes", 1, "NO PAD"},
	{"utf8_bin", "utf8", 83, "", "Yes", 1, "NO PAD"},
	{"utf8mb4_general_ci", "utf8mb4", 45, "YES", "Yes", 1, "PAD SPACE"},
	{"utf8mb4_bin", "utf8mb4", 46, "", "Yes", 1, "NO PAD"},
}
