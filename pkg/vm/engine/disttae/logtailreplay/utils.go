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
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"regexp"
	"strings"
)

var (
	dataObjectListPattern      = regexp.MustCompile(`_\d+_data_meta`)
	tombstoneObjectListPattern = regexp.MustCompile(`_\d+_tombstone_meta`)
)

func IsMetaEntry(tblName string) bool {
	return IsDataObjectList(tblName) || IsTombstoneObjectList(tblName)
}

func IsDataObjectList(tblName string) bool {
	return dataObjectListPattern.MatchString(tblName)
}

func IsTombstoneObjectList(tblName string) bool {
	return tombstoneObjectListPattern.MatchString(tblName)
}

func IsTransferredDels(name string) bool {
	return strings.HasPrefix(name, "trans_del")
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
