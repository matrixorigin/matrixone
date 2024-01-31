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

var Collations []*Collation = []*Collation{
	{"utf8_general_ci", "utf8", 33, "", "Yes", 1, "PAD SPACE"},
	{"binary", "binary", 63, "YES", "Yes", 1, "NO PAD"},
	{"utf8_unicode_ci", "utf8", 192, "", "Yes", 1, "PAD SPACE"},
	{"utf8_bin", "utf8", 83, "YES", "Yes", 1, "NO PAD"},
	{"utf8mb4_general_ci", "utf8mb4", 45, "", "Yes", 1, "PAD SPACE"},
	{"utf8mb4_unicode_ci", "utf8mb4", 224, "", "Yes", 1, "PAD SPACE"},
	{"utf8mb4_bin", "utf8mb4", 46, "YES", "Yes", 1, "NO PAD"},
	{"utf8mb4_0900_bin", "utf8mb4", 309, "", "Yes", 1, "NO PAD"},
	{"utf8mb4_0900_ai_ci", "utf8mb4", 255, "", "Yes", 0, "NO PAD"},
	{"utf8mb4_de_pb_0900_ai_ci", "utf8mb4", 256, "", "Yes", 0, "NO PAD"},
	{"utf8mb4_is_0900_ai_ci", "utf8mb4", 257, "", "Yes", 0, "NO PAD"},
	{"utf8mb4_lv_0900_ai_ci", "utf8mb4", 258, "", "Yes", 0, "NO PAD"},
}
