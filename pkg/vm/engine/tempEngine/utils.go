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

package tempengine

import (
	"bytes"
	"strings"
)

func getTblName(db_tblName string) string {
	return strings.Split(db_tblName, "-")[1]
}

func copyData(data []byte, buf *bytes.Buffer) []byte {
	buf.Reset()
	if len(data) > buf.Cap() {
		buf.Grow(len(data))
	}
	res := buf.Bytes()[:len(data)]
	copy(res, data)
	return res
}
