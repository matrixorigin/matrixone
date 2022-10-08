// Copyright 2022 Matrix Origin
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

package common

type CompressType uint8

const (
	Plain CompressType = iota
	Lz4
)

func Compress(raw []byte, ctyp CompressType) []byte {
	return raw
}

func Decompress(src []byte, dst []byte, ctyp CompressType) error {
	copy(dst, src)
	return nil
}
