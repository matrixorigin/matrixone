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

package data

import "errors"

var (
	ErrAppendableSegmentNotFound = errors.New("tae data: no appendable segment")
	ErrAppendableBlockNotFound   = errors.New("tae data: no appendable block")
	ErrNotAppendable             = errors.New("tae data: not appendable")
	ErrUpdateUniqueKey           = errors.New("tae data: update unique key")
	ErrUpdatePhyAddrKey          = errors.New("tae data: update physical address key")
	ErrStaleRequest              = errors.New("tae data: stale request")

	ErrPossibleDuplicate = errors.New("tae data: possible duplicate")
	ErrDuplicate         = errors.New("tae data: duplicate")
	ErrNotFound          = errors.New("tae data: not found")
	ErrWrongType         = errors.New("tae data: wrong data type")
)
