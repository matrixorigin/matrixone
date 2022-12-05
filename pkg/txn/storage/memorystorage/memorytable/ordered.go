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

package memorytable

// Ordered represents ordered value
type Ordered[To any] interface {
	Less(to To) bool
}

type min struct{}

// Min is a value that smaller than all values except Min itself
var Min min

type max struct{}

// Max is a value that greater than all values except Max itself
var Max max
