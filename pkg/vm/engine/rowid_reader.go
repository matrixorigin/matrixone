// Copyright 2026 Matrix Origin
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

package engine

import "errors"

const (
	MaxRowIDReadRows  = 32768
	MaxRowIDReadBytes = 8 << 20
)

var ErrRowIDReadLimit = errors.New("historical row lookup exceeds refresh scratch limit")

// RowIDReadBudget is shared across all snapshot groups in one consumer chunk.
// Charge before copying values out of a borrowed engine batch. This limits
// transient owned data; persistent relation storage uses the engine's quotas.
type RowIDReadBudget struct{ RemainingBytes int }

func (b *RowIDReadBudget) Charge(bytes int) error {
	if b == nil || bytes < 0 || bytes > b.RemainingBytes || b.RemainingBytes > MaxRowIDReadBytes {
		return ErrRowIDReadLimit
	}
	b.RemainingBytes -= bytes
	return nil
}
