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

package fz

import (
	"time"

	"github.com/google/uuid"
)

type CreatedAt string

func (_ Def) CreatedAt() CreatedAt {
	return CreatedAt(time.Now().Format(time.RFC3339))
}

func (_ Def) CreatedAtConfigItem(
	t CreatedAt,
) ConfigItems {
	return ConfigItems{t}
}

type UUID = uuid.UUID

func (_ Def) UUID() UUID {
	return uuid.New()
}

func (_ Def) UUIDConfigItem(
	id uuid.UUID,
) ConfigItems {
	return ConfigItems{id}
}
