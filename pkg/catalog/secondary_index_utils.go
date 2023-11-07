// Copyright 2023 Matrix Origin
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

package catalog

import (
	"fmt"
	"strings"
)

// Index Algorithm names
const (
	MoIndexDefaultAlgo = ""        // used by UniqueIndex or default SecondaryIndex
	MoIndexBTreeAlgo   = "btree"   // used for Mocking MySQL behaviour.
	MoIndexIvfFlatAlgo = "ivfflat" // used for IVF flat index on Vector/Array columns
)

func IsDefaultIndexAlgo(algo string) bool {
	_algo := strings.ToLower(strings.TrimSpace(algo))
	return _algo == MoIndexDefaultAlgo || _algo == MoIndexBTreeAlgo
}

// ------------------------[START] Aliaser------------------------

// This class is used by "secondary index" to resolve the "programmatically generated PK" appended to the
// end of the index key "__mo_index_idx_col".

const (
	AliasPrefix = "__mo_alias_"
)

func CreateAlias(column string) string {
	return fmt.Sprintf("%s%s", AliasPrefix, column)
}

func ResolveAlias(alias string) string {
	return strings.TrimPrefix(alias, AliasPrefix)
}

func IsAlias(column string) bool {
	return strings.HasPrefix(column, AliasPrefix)
}

// ------------------------[END] Aliaser------------------------
