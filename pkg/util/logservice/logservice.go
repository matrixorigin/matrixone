// Copyright 2021 - 2024 Matrix Origin
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

package logservice

import (
	"strings"
)

// GetLocalityFromStr updates the locality of non-voting replicas.
// Its format should be like: key[:value];[key2[:value2]][;]. The key will be
// removed if no values specified.
// such as:
//
//	key1:value1
//	key1
//	key1:value1;key2:value2:key3
func GetLocalityFromStr(str string) map[string]string {
	if str == "" {
		return nil
	}
	loc := make(map[string]string)
	kvs := strings.Split(str, ";")
	for _, kv := range kvs {
		pair := strings.Split(kv, ":")
		if len(pair) == 2 {
			loc[pair[0]] = pair[1]
		} else if len(pair) == 1 {
			loc[pair[0]] = ""
		}
	}
	return loc
}
