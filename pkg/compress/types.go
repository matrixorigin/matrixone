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

package compress

import "fmt"

const (
	None = iota
	Lz4
)

type T uint8

func (t T) String() string {
	switch t {
	case None:
		return "None"
	case Lz4:
		return "LZ4"
	}
	return fmt.Sprintf("unexpected compress type: %d", t)
}
