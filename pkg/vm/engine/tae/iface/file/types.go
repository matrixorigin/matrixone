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

package file

import (
	"errors"
	"io"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

var (
	ErrInvalidParam = errors.New("tae: invalid param")
	ErrInvalidName  = errors.New("tae: invalid name")
)

type Base interface {
	common.IRef
	io.Closer
	Fingerprint() *common.ID
}

type SegmentFactory interface {
	Build(dir string, id uint64) Segment
	EncodeName(id uint64) string
	DecodeName(name string) (id uint64, err error)
}
