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

package logtailreplay

import (
	"regexp"
	"sort"

	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
)

var metaTableMatchRegexp = regexp.MustCompile(`\_\d+\_(meta|seg)`)
var blkTableMatchRegexp = regexp.MustCompile(`\_\d+\_meta`)
var objTableMatchRegexp = regexp.MustCompile(`\_\d+\_obj`)

func IsMetaTable(name string) bool {
	return metaTableMatchRegexp.MatchString(name)
}

func IsBlkTable(name string) bool {
	return blkTableMatchRegexp.MatchString(name)
}

func IsObjTable(name string) bool {
	return objTableMatchRegexp.MatchString(name)
}

func mustVectorFromProto(v api.Vector) *vector.Vector {
	ret, err := vector.ProtoVectorToVector(v)
	if err != nil {
		panic(err)
	}
	return ret
}

func mustVectorToProto(v *vector.Vector) api.Vector {
	ret, err := vector.VectorToProtoVector(v)
	if err != nil {
		panic(err)
	}
	return ret
}

func sortUnique[T any](slice []T, cmp func(T, T) int) []T {
	if len(slice) == 0 {
		return nil
	}
	sort.Slice(slice, func(i, j int) bool {
		return cmp(slice[i], slice[j]) < 0
	})
	res := slice[:0]
	for i := 0; i < len(slice)-1; i++ {
		if cmp(slice[i], slice[i+1]) != 0 {
			res = append(res, slice[i])
		}
	}
	res = append(res, slice[len(slice)-1])
	return res
}
