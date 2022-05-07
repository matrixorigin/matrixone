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

package aoedb

import "strconv"

var IDToNameFactory = new(idToNameFactory)

type DBNameFactory interface {
	Encode(interface{}) string
	Decode(string) interface{}
}

type idToNameFactory struct{}

func (f *idToNameFactory) Encode(v interface{}) string {
	shardID := v.(uint64)
	return strconv.FormatUint(shardID, 10)
}

func (f *idToNameFactory) Decode(name string) (interface{}, error) {
	return strconv.ParseUint(name, 10, 64)
}
