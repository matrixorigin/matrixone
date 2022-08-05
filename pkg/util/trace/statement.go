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

package trace

import (
	"time"
)

type Statement struct {
	StatementID          uint64
	SessionID            uint64
	TransactionID        uint64
	Account              string
	User                 string
	Host                 string
	Database             string
	Statement            string
	StatementFingerprint string
	StatementTag         string
	RequestAt            time.Time
}

type StatementOption interface {
	Apply(*Statement)
}

type StatementOptionFunc func(*Statement)

func WithRequestAt(t time.Time) StatementOptionFunc {
	return StatementOptionFunc(func(s *Statement) {
		s.RequestAt = t
	})
}

func CollectStatement(s Statement) error {
	return nil
}
