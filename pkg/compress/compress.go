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

import (
	"github.com/pierrec/lz4/v4"
)

var Algorithms map[string]int = map[string]int{
	"lz4":  Lz4,
	"none": None,
}

func Compress(src, dst []byte, typ int) ([]byte, error) {
	switch typ {
	case Lz4:
		n, err := lz4.CompressBlock(src, dst, nil)
		if err != nil {
			return nil, err
		}
		return dst[:n], nil
	}
	return nil, nil
}

func Decompress(src, dst []byte, typ int) ([]byte, error) {
	switch typ {
	case Lz4:
		n, err := lz4.UncompressBlock(src, dst)
		if err != nil {
			return nil, err
		}
		return dst[:n], nil
	}
	return nil, nil
}
