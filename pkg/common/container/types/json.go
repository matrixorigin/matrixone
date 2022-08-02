// Copyright 2022 Matrix Origin
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

package types

import (
	bytejson2 "github.com/matrixorigin/matrixone/pkg/common/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func ParseNumValToByteJson(num *tree.NumVal) (bytejson2.ByteJson, error) {
	val := num.String()
	return ParseStringToByteJson(val)
}

func ParseStringToByteJson(str string) (bytejson2.ByteJson, error) {
	return bytejson2.ParseFromString(str)
}
func ParseSliceToByteJson(dt []byte) (bytejson2.ByteJson, error) {
	return bytejson2.ParseFromByteSlice(dt)
}
