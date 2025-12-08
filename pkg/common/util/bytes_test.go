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

package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCopyBytes(t *testing.T) {
	tests := []struct {
		name     string
		src      []byte
		needCopy bool
		want     []byte
		wantSame bool // whether the returned slice is the same as src (when needCopy=false)
	}{
		{
			name:     "empty slice, needCopy=true",
			src:      []byte{},
			needCopy: true,
			want:     []byte{},
			wantSame: false,
		},
		{
			name:     "empty slice, needCopy=false",
			src:      []byte{},
			needCopy: false,
			want:     []byte{},
			wantSame: true,
		},
		{
			name:     "non-empty slice, needCopy=true",
			src:      []byte{'a', 'b', 'c'},
			needCopy: true,
			want:     []byte{'a', 'b', 'c'},
			wantSame: false,
		},
		{
			name:     "non-empty slice, needCopy=false",
			src:      []byte{'a', 'b', 'c'},
			needCopy: false,
			want:     []byte{'a', 'b', 'c'},
			wantSame: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CloneBytesIf(tt.src, tt.needCopy)
			assert.Equal(t, tt.want, got, "CopyBytes(%v, %v)", tt.src, tt.needCopy)

			// Check if the returned slice is the same as src when needCopy=false
			if tt.needCopy {
				// When needCopy=true, the slice should be a copy
				if len(tt.src) > 0 {
					assert.NotSame(t, &tt.src[0], &got[0], "CopyBytes should return a new slice when needCopy=true")
				}
			} else {
				// When needCopy=false, the slice should be the same
				if len(tt.src) > 0 {
					assert.Same(t, &tt.src[0], &got[0], "CopyBytes should return the same slice when needCopy=false")
				}
			}

			// Verify that modifying the original doesn't affect the copy when needCopy=true
			if tt.needCopy && len(tt.src) > 0 {
				original := make([]byte, len(tt.src))
				copy(original, tt.src)
				tt.src[0] = 'x'
				assert.Equal(t, original, got, "CopyBytes should return an independent copy when needCopy=true")
			}
		})
	}
}
