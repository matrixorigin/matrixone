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

	"github.com/matrixorigin/matrixone/pkg/logservice/pb/rpc"
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
		// dragonboat doesn't consider out of range as an error, it is treated more
		// like a possible output.
		{ErrOutOfRange, rpc.ErrorCode_OutOfRange},
		{dragonboat.ErrPayloadTooBig, rpc.ErrorCode_InvalidPayloadSize},
		{dragonboat.ErrRejected, rpc.ErrorCode_Rejected},
		{dragonboat.ErrShardNotReady, rpc.ErrorCode_ShardNotReady},
		{dragonboat.ErrSystemBusy, rpc.ErrorCode_ShardNotReady},
		{dragonboat.ErrClosed, rpc.ErrorCode_SystemClosed},
	}
}

func toErrorCode(err error) rpc.ErrorCode {
	if err == nil {
		return rpc.ErrorCode_NoError
	}
	for _, rec := range errorMappings {
		if errors.Is(err, rec.err) {
			return rec.code
		}
	}
	return rpc.ErrorCode_OtherSystemError
}
