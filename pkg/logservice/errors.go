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
	"github.com/cockroachdb/errors"
	"github.com/lni/dragonboat/v4"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

var (
	ErrUnknownError = moerr.NewError(moerr.INVALID_STATE, "unknown error")
)

type errorToCode struct {
	err     error
	code    logservice.ErrorCode
	reverse bool
}

var errorToCodeMappings = getErrorToCodeMapping()

func getErrorToCodeMapping() []errorToCode {
	return []errorToCode{
		{dragonboat.ErrTimeout, logservice.ErrorCode_Timeout, true},
		{dragonboat.ErrShardNotFound, logservice.ErrorCode_InvalidShard, true},
		// TODO: why ErrTimeoutTooSmall is possible
		{dragonboat.ErrTimeoutTooSmall, logservice.ErrorCode_Timeout, false},
		{dragonboat.ErrPayloadTooBig, logservice.ErrorCode_InvalidPayloadSize, true},
		{dragonboat.ErrRejected, logservice.ErrorCode_Rejected, true},
		{dragonboat.ErrShardNotReady, logservice.ErrorCode_ShardNotReady, true},
		{dragonboat.ErrSystemBusy, logservice.ErrorCode_ShardNotReady, false},
		{dragonboat.ErrClosed, logservice.ErrorCode_SystemClosed, true},

		{ErrInvalidTruncateIndex, logservice.ErrorCode_IndexAlreadyTruncated, true},
		{ErrNotLeaseHolder, logservice.ErrorCode_NotLeaseHolder, true},
		{ErrOutOfRange, logservice.ErrorCode_OutOfRange, true},
	}
}

func toErrorCode(err error) (logservice.ErrorCode, string) {
	if err == nil {
		return logservice.ErrorCode_NoError, ""
	}
	for _, rec := range errorToCodeMappings {
		if errors.Is(err, rec.err) {
			plog.Errorf("error: %v, converted to code %d", err, rec.code)
			return rec.code, ""
		}
	}
	plog.Errorf("unrecognized error %v, converted to %d", err,
		logservice.ErrorCode_OtherSystemError)
	return logservice.ErrorCode_OtherSystemError, err.Error()
}

func toError(resp logservice.Response) error {
	if resp.ErrorCode == logservice.ErrorCode_NoError {
		return nil
	} else if resp.ErrorCode == logservice.ErrorCode_OtherSystemError {
		return errors.Wrapf(ErrUnknownError, resp.ErrorMessage)
	}

	for _, rec := range errorToCodeMappings {
		if rec.code == resp.ErrorCode && rec.reverse {
			return rec.err
		}
	}
	plog.Panicf("Unknown error code: %d", resp.ErrorCode)
	// will never reach here
	return nil
}
