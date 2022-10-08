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
	"fmt"
	"net"

	"github.com/cockroachdb/errors"
	"github.com/lni/dragonboat/v4"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

type errorToCode struct {
	err     error
	code    uint16
	reverse bool
}

var errorToCodeMappings = getErrorToCodeMapping()

// XXX This is strange code mapping.   Esp, the those mapped to
// timeout -- per dragonbat, ErrTimeout is temp, but TimeoutTooSmall
// and InvalidDeadline are not temp.
func getErrorToCodeMapping() []errorToCode {
	return []errorToCode{
		{dragonboat.ErrTimeout, moerr.ErrDragonboatTimeout, true},
		{dragonboat.ErrTimeoutTooSmall, moerr.ErrDragonboatTimeout, false},
		{dragonboat.ErrInvalidDeadline, moerr.ErrDragonboatTimeout, false},
		{dragonboat.ErrPayloadTooBig, moerr.ErrDragonboatInvalidPayloadSize, true},
		{dragonboat.ErrRejected, moerr.ErrDragonboatRejected, true},
		{dragonboat.ErrShardNotReady, moerr.ErrDragonboatShardNotReady, true},
		{dragonboat.ErrSystemBusy, moerr.ErrDragonboatShardNotReady, false},
		{dragonboat.ErrClosed, moerr.ErrDragonboatSystemClosed, true},
		{dragonboat.ErrInvalidRange, moerr.ErrDragonboatInvalidRange, true},
		{dragonboat.ErrShardNotFound, moerr.ErrDragonboatShardNotFound, true},
	}
}

func toErrorCode(err error) (uint32, string) {
	if err == nil {
		return uint32(moerr.Ok), ""
	}
	for _, rec := range errorToCodeMappings {
		if errors.Is(err, rec.err) {
			logutil.Error(fmt.Sprintf("error: %v, converted to code %d", err, rec.code))
			return uint32(rec.code), ""
		}
	}

	merr, ok := err.(*moerr.Error)
	if ok {
		return uint32(merr.ErrorCode()), ""
	}
	return uint32(moerr.ErrDragonboatOtherSystemError), err.Error()
}

// toError reverse the response to dragonboat error.
func toError(resp pb.Response) error {
	if resp.ErrorCode == uint32(moerr.Ok) {
		return nil
	} else if resp.ErrorCode == uint32(moerr.ErrDragonboatOtherSystemError) {
		return moerr.NewDragonboatOtherSystemError(resp.ErrorMessage)
	}
	// Mapped errors, not that we return a dragonboat error in these cases.
	for _, rec := range errorToCodeMappings {
		if uint32(rec.code) == resp.ErrorCode && rec.reverse {
			return rec.err
		}
	}
	// Three of our own errors.
	if resp.ErrorCode == uint32(moerr.ErrNoHAKeeper) {
		return moerr.NewNoHAKeeper()
	} else if resp.ErrorCode == uint32(moerr.ErrInvalidTruncateLsn) {
		return moerr.NewInvalidTruncateLsn(0, 0)
	} else if resp.ErrorCode == uint32(moerr.ErrNotLeaseHolder) {
		// hoder id get lost?
		return moerr.NewNotLeaseHolder(0xDEADBEEF)
	} else {
		// will logger.Panicf panic?
		panic(moerr.NewInternalError("unknown error code: %d", resp.ErrorCode))
	}
	// will never reach here
}

func isTempError(err error) bool {
	// non mo errors reached here, we handle first.
	if dragonboat.IsTempError(err) {
		return true
	}

	// XXX this error, while dragonboat says it is not temp
	// we say yes.
	if errors.Is(err, dragonboat.ErrShardNotFound) {
		return true
	}

	//
	if _, ok := err.(net.Error); ok {
		return true
	}

	// How about those mapped from dragonboat errors?
	// ErrDragonboatTimeout
	// ErrDragonboatShardNotReady
	// ErrDragonboatSystemClosed
	//
	// But, dragonboad ShardNotFound was not temp, but the code
	// says we treat it as temp.
	//
	// Note that drgonboat errors are handled before moerr.
	if moerr.IsMoErrCode(err, moerr.ErrNoHAKeeper) ||
		moerr.IsMoErrCode(err, moerr.ErrDragonboatShardNotFound) {
		return true
	}

	return false
}
