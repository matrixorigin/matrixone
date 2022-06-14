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

package segmentio

type fileStat struct {
	name       string
	size       int64
	originSize int64
	algo       uint8
}

func (stat *fileStat) Name() string      { return stat.name }
func (stat *fileStat) Size() int64       { return stat.size }
func (stat *fileStat) OriginSize() int64 { return stat.originSize }
func (stat *fileStat) CompressAlgo() int { return int(stat.algo) }
