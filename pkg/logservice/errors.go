// Copyright 2022 MatrixOrigin.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package logservice

import (
	"github.com/cockroachdb/errors"
	"github.com/lni/dragonboat/v4"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logservice/pb/rpc"
)

var (
	ErrUnknownError = moerr.NewError(moerr.INVALID_STATE, "unknown error")
)

type errMapping struct {
	err  error
	code rpc.ErrorCode
}

var errorMappings = getErrorMapping()

func getErrorMapping() []errMapping {
	return []errMapping{
		{dragonboat.ErrTimeout, rpc.ErrorCode_Timeout},
		{dragonboat.ErrShardNotFound, rpc.ErrorCode_InvalidShard},
		{dragonboat.ErrTimeoutTooSmall, rpc.ErrorCode_InvalidTimeout},
		{dragonboat.ErrPayloadTooBig, rpc.ErrorCode_InvalidPayloadSize},
		{dragonboat.ErrRejected, rpc.ErrorCode_Rejected},
		{dragonboat.ErrShardNotReady, rpc.ErrorCode_ShardNotReady},
		{dragonboat.ErrSystemBusy, rpc.ErrorCode_ShardNotReady},
		{dragonboat.ErrClosed, rpc.ErrorCode_SystemClosed},

		{ErrInvalidTruncateIndex, rpc.ErrorCode_IndexAlreadyTruncated},
		{ErrNotLeaseHolder, rpc.ErrorCode_NotLeaseHolder},
		{ErrOutOfRange, rpc.ErrorCode_OutOfRange},
	}
}

func toErrorCode(err error) (rpc.ErrorCode, string) {
	if err == nil {
		return rpc.ErrorCode_NoError, ""
	}
	for _, rec := range errorMappings {
		if errors.Is(err, rec.err) {
			plog.Errorf("error %v converted to %d", err, rec.code)
			return rec.code, ""
		}
	}
	plog.Errorf("unrecognized error %v, converted to %d", err,
		rpc.ErrorCode_OtherSystemError)
	return rpc.ErrorCode_OtherSystemError, err.Error()
}

func toError(resp rpc.Response) error {
	if resp.ErrorCode == rpc.ErrorCode_NoError {
		return nil
	} else if resp.ErrorCode == rpc.ErrorCode_OtherSystemError {
		return errors.Wrapf(ErrUnknownError, resp.ErrorMessage)
	}

	for _, rec := range errorMappings {
		if rec.code == resp.ErrorCode {
			return rec.err
		}
	}
	plog.Panicf("Unknown error code: %d", resp.ErrorCode)
	// will never reach here
	return nil
}
