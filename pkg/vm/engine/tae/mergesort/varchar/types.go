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

package varchar

import (
	"bytes"
)

type sortElem struct {
	data []byte
	idx  uint32
}

type sortSlice []sortElem

func (x sortSlice) Less(i, j int) bool {
	return bytes.Compare(x[i].data, x[j].data) < 0
}

func (x sortSlice) Swap(i, j int) { x[i], x[j] = x[j], x[i] }

type heapElem struct {
	data []byte
	src  uint32
	next uint32
}

type heapSlice []heapElem

func (x heapSlice) Less(i, j int) bool {
	return bytes.Compare(x[i].data, x[j].data) < 0
}

func (x heapSlice) Swap(i, j int) { x[i], x[j] = x[j], x[i] }
