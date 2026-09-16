// Copyright 2026 Matrix Origin
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

package collation

import (
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/collations/colldata"
)

const (
	mysqlUTF8MB40900AICollationID  collations.ID = 255
	mysqlUTF8MB40900BinCollationID collations.ID = 309
)

var (
	uca0900AI  = colldata.Lookup(mysqlUTF8MB40900AICollationID)
	uca0900Bin = colldata.Lookup(mysqlUTF8MB40900BinCollationID)
)

func init() {
	if uca0900AI == nil || uca0900Bin == nil {
		panic("vitess collation registry does not contain utf8mb4 0900 collations")
	}
}

// UCA0900AIWeight returns the unpadded MySQL UCA 9.0 weight string. The
// result is a bytewise comparison key and is intentionally not reversible.
func UCA0900AIWeight(dst, value []byte) []byte {
	return uca0900AI.WeightString(dst, value, 0)
}

// UCA0900AICollate is kept beside the weight adapter so tests can compare the
// stored-key order against the independent library comparator.
func UCA0900AICollate(left, right []byte) int {
	return uca0900AI.Collate(left, right, false)
}

func UCA0900BinCollate(left, right []byte) int {
	return uca0900Bin.Collate(left, right, false)
}

// UCA0900AIMatch applies the collation-aware SQL wildcard matcher. It keeps
// LIKE in character space; converting the pattern itself to a weight string
// would make '%' and '_' lose their wildcard meaning.
func UCA0900AIMatch(pattern, value []byte, escape rune) bool {
	return uca0900AI.Wildcard(pattern, 0, 0, escape).Match(value)
}
