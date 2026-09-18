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

package taestorage

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/stretchr/testify/require"
)

func TestWriteRejectsRetiredCommitMerge(t *testing.T) {
	s := &taeStorage{}

	result, err := s.Write(
		context.Background(),
		txn.TxnMeta{},
		uint32(api.OpCode_OpCommitMerge),
		nil,
	)
	require.Nil(t, result)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported), err)
}
