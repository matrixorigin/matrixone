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

package logservice

import (
	"context"
	"testing"

	"github.com/lni/dragonboat/v4"
	"github.com/stretchr/testify/assert"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

func TestTimeoutError(t *testing.T) {
	code, _ := toErrorCode(dragonboat.ErrTimeout)
	assert.Equal(t, moerr.ErrDragonboatTimeout, uint16(code))
	code, _ = toErrorCode(dragonboat.ErrTimeoutTooSmall)
	assert.Equal(t, moerr.ErrDragonboatTimeout, uint16(code))
	code, _ = toErrorCode(dragonboat.ErrInvalidDeadline)
	assert.Equal(t, moerr.ErrDragonboatTimeout, uint16(code))
}

func TestErrorConversion(t *testing.T) {
	ctx := context.TODO()
	mappings := getErrorToCodeMapping()
	for _, rec := range mappings {
		if rec.reverse {
			code, msg := toErrorCode(rec.err)
			resp := pb.Response{
				ErrorCode:    uint32(code),
				ErrorMessage: msg,
			}
			err := toError(ctx, resp)
			assert.Equal(t, err, rec.err)
		}
	}
}

func TestUnknownErrorIsHandled(t *testing.T) {
	ctx := context.TODO()
	err := moerr.NewDragonboatOtherSystemError(ctx, "test error")
	code, _ := toErrorCode(err)
	assert.Equal(t, moerr.ErrDragonboatOtherSystemError, uint16(code))
}

func TestIsTempError(t *testing.T) {
	ctx := context.TODO()
	tests := []struct {
		err  error
		temp bool
	}{
		{dragonboat.ErrInvalidOperation, false},
		{dragonboat.ErrInvalidAddress, false},
		{dragonboat.ErrInvalidSession, false},
		{dragonboat.ErrTimeoutTooSmall, false},
		{dragonboat.ErrPayloadTooBig, false},
		{dragonboat.ErrSystemBusy, true},
		{dragonboat.ErrShardClosed, true},
		{dragonboat.ErrShardNotInitialized, true},
		{dragonboat.ErrTimeout, true},
		{dragonboat.ErrClosed, true},
		{dragonboat.ErrCanceled, false},
		{dragonboat.ErrRejected, false},
		{dragonboat.ErrShardNotReady, true},
		{dragonboat.ErrInvalidTarget, false},
		{dragonboat.ErrInvalidRange, false},
		{dragonboat.ErrShardNotFound, true},

		{moerr.NewNoHAKeeper(ctx), true},
		{moerr.NewNoDB(ctx), false},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.temp, isTempError(tt.err))
	}
}
