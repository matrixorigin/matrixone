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

package catalog

import "errors"

var (
	ErrNotFound   = errors.New("tae catalog: not found")
	ErrDuplicate  = errors.New("tae catalog: duplicate")
	ErrCheckpoint = errors.New("tae catalog: checkpoint")

	ErrValidation = errors.New("tae catalog: validataion")

	ErrStopCurrRecur = errors.New("tae catalog: stop current recursion")
)
