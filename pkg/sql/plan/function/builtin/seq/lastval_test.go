// Copyright 2021 - 2022 Matrix Origin
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

package seq

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestLastval(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	proc := testutil.NewProc()
	proc.SessionInfo.SeqLastValue = make([]string, 1)
	proc.SessionInfo.SeqLastValue[0] = "999"

	tests := []struct {
		name    string
		vectors []*vector.Vector
	}{
		{
			name:    "test01",
			vectors: []*vector.Vector{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, err := Lastval(tt.vectors, proc)
			if err != nil {
				t.Errorf("Lastval() error = %v", err)
				return
			}
			require.Nil(t, err)
			require.Equal(t, "999", r.String())
		})
	}
}

func TestLastvalNotCached(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	proc := testutil.NewProc()
	proc.SessionInfo.SeqLastValue = make([]string, 1)
	proc.SessionInfo.SeqLastValue[0] = ""

	tests := []struct {
		name    string
		vectors []*vector.Vector
	}{
		{
			name:    "test01",
			vectors: []*vector.Vector{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Lastval(tt.vectors, proc)
			require.Equal(t, moerr.NewInternalError(proc.Ctx, "Last value of current session is not initialized."), err)
		})
	}
}
