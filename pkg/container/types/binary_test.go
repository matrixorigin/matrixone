// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package types

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/require"
)

func TestBitwiseLengthMismatchReturnsTypedError(t *testing.T) {
	for _, test := range []struct {
		name string
		fn   func([]byte, []byte, []byte) error
	}{
		{name: "and", fn: BitAnd},
		{name: "or", fn: BitOr},
		{name: "xor", fn: BitXor},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := test.fn(make([]byte, 1), []byte{0x01}, []byte{0x00, 0x01})
			require.Error(t, err)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidBitwiseOperandsSize))
		})
	}
}
