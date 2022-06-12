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
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

var (
	ErrUnknownError = moerr.NewError(moerr.INVALID_STATE, "unknown error")
)

type errorToCode struct {
	err     error
	code    pb.ErrorCode
	reverse bool
}

var errorToCodeMappings = getErrorToCodeMapping()

func getErrorToCodeMapping() []errorToCode {
	return []errorToCode{
		{dragonboat.ErrTimeout, pb.ErrorCode_Timeout, true},
		{dragonboat.ErrShardNotFound, pb.ErrorCode_InvalidShard, true},
		// TODO: why ErrTimeoutTooSmall is possible
		{dragonboat.ErrTimeoutTooSmall, pb.ErrorCode_Timeout, false},
		{dragonboat.ErrPayloadTooBig, pb.ErrorCode_InvalidPayloadSize, true},
		{dragonboat.ErrRejected, pb.ErrorCode_Rejected, true},
		{dragonboat.ErrShardNotReady, pb.ErrorCode_ShardNotReady, true},
		{dragonboat.ErrSystemBusy, pb.ErrorCode_ShardNotReady, false},
		{dragonboat.ErrClosed, pb.ErrorCode_SystemClosed, true},

		{ErrInvalidTruncateIndex, pb.ErrorCode_IndexAlreadyTruncated, true},
		{ErrNotLeaseHolder, pb.ErrorCode_NotLeaseHolder, true},
		{ErrOutOfRange, pb.ErrorCode_OutOfRange, true},
	}
}

func toErrorCode(err error) (pb.ErrorCode, string) {
	if err == nil {
		return pb.ErrorCode_NoError, ""
	}
	for _, rec := range errorToCodeMappings {
		if errors.Is(err, rec.err) {
			plog.Errorf("error: %v, converted to code %d", err, rec.code)
			return rec.code, ""
		}
	}
	plog.Errorf("unrecognized error %v, converted to %d", err,
		pb.ErrorCode_OtherSystemError)
	return pb.ErrorCode_OtherSystemError, err.Error()
}

func toError(resp pb.Response) error {
	if resp.ErrorCode == pb.ErrorCode_NoError {
		return nil
	} else if resp.ErrorCode == pb.ErrorCode_OtherSystemError {
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
