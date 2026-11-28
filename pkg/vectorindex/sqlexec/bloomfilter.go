// Copyright 2022 Matrix Origin
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

package sqlexec

import (
	"context"
	"encoding/hex"
	"math"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap/keycodec"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
)

// UniqueJoinKeysStatus describes the terminal state of a unique-join-keys
// runtime filter. PASS means that the producer could not safely construct an
// exact filter and the consumer must fall back to an unfiltered plan. DROP
// means that the build side is empty and the consumer can return no rows.
type UniqueJoinKeysStatus uint8

const (
	UniqueJoinKeysNone UniqueJoinKeysStatus = iota
	UniqueJoinKeysAvailable
	UniqueJoinKeysPass
	UniqueJoinKeysDrop
)

// WaitUniqueJoinKeys blocks until it receives a RuntimeFilter_UNIQUEJOINKEYS message
// that matches sqlproc.RuntimeFilterSpecs (if any). It returns the raw serialized
// unique join key bytes from the build side.
//
// The caller is responsible for deserializing the bytes and deciding how to use
// them (e.g. exact pk IN filter vs a membership filter based on its own threshold).
func WaitUniqueJoinKeys(sqlproc *SqlProcess) (*keycodec.KeyPayload, error) {
	data, status, err := waitUniqueJoinKeys(sqlproc, false)
	if status != UniqueJoinKeysAvailable {
		return nil, err
	}
	return data, err
}

// WaitUniqueJoinKeysWithStatus is the status-preserving form used by consumers
// whose correctness depends on knowing whether the producer sent PASS or DROP.
// The legacy WaitUniqueJoinKeys API intentionally retains its old fail-open
// behavior for callers that only use an optional membership filter.
func WaitUniqueJoinKeysWithStatus(sqlproc *SqlProcess) (*keycodec.KeyPayload, UniqueJoinKeysStatus, error) {
	return waitUniqueJoinKeys(sqlproc, true)
}

func waitUniqueJoinKeys(sqlproc *SqlProcess, preserveStatus bool) (*keycodec.KeyPayload, UniqueJoinKeysStatus, error) {
	if sqlproc.Proc == nil {
		return nil, UniqueJoinKeysNone, nil
	}

	if len(sqlproc.RuntimeFilterSpecs) == 0 {
		return nil, UniqueJoinKeysNone, nil
	}
	spec := sqlproc.RuntimeFilterSpecs[0]
	if !spec.UseMembershipFilter {
		return nil, UniqueJoinKeysNone, nil
	}

	msgReceiver := message.NewMessageReceiver(
		[]int32{spec.Tag},
		message.AddrBroadCastOnCurrentCN(),
		sqlproc.Proc.GetMessageBoard(),
	)
	msgs, ctxDone, err := msgReceiver.ReceiveMessage(true, sqlproc.GetContext())
	if err != nil || ctxDone {
		return nil, UniqueJoinKeysNone, err
	}

	for i := range msgs {
		m, ok := msgs[i].(message.RuntimeFilterMessage)
		if !ok {
			continue
		}
		switch m.Typ {
		case message.RuntimeFilter_UNIQUEJOINKEYS:
			if m.Keys == nil || !m.Keys.Retain() {
				return nil, UniqueJoinKeysNone, moerr.NewInvalidStateNoCtx("runtime key payload was released")
			}
			return m.Keys, UniqueJoinKeysAvailable, nil
		case message.RuntimeFilter_PASS:
			if !preserveStatus {
				continue
			}
			return nil, UniqueJoinKeysPass, nil
		case message.RuntimeFilter_DROP:
			if !preserveStatus {
				continue
			}
			return nil, UniqueJoinKeysDrop, nil
		}
	}

	return nil, UniqueJoinKeysNone, nil
}

// BuildExactPkFilter converts a vector of primary key values into a comma-separated
// SQL literal string suitable for use in an IN (...) clause.
func BuildExactPkFilterKeys(ctx context.Context, keys *keycodec.KeyPayload) (string, error) {
	var parts []string
	err := keys.Visit(ctx, func(v *vector.Vector) error {
		part, err := BuildExactPkFilter(ctx, v)
		if err == nil && part != "" {
			parts = append(parts, part)
		}
		return err
	})
	return strings.Join(parts, ","), err
}

func BuildExactPkFilter(ctx context.Context, vec *vector.Vector) (string, error) {
	var buf []byte
	rowCount := vec.Length()
	for i := 0; i < rowCount; i++ {
		if vec.IsNull(uint64(i)) {
			continue
		}
		if len(buf) > 0 {
			buf = append(buf, ',')
		}
		var err error
		buf, err = AppendVectorSQLLiteral(ctx, vec, i, buf)
		if err != nil {
			return "", err
		}
	}
	return string(buf), nil
}

// AppendVectorSQLLiteral appends the SQL literal representation of vec[row] to buf.
func AppendVectorSQLLiteral(ctx context.Context, vec *vector.Vector, row int, buf []byte) ([]byte, error) {
	typ := vec.GetType()
	if !vector.SupportsAny(typ.Oid) {
		return nil, moerr.NewInternalErrorf(ctx, "IVF membership filter: unsupported pk type %d", typ.Oid)
	}
	return types.AppendMOSQLLiteral(buf, *typ, vector.ExportValue(vec, row))
}

// AppendHex appends hex-encoded binary literal to buf.
func AppendHex(dst []byte, src []byte) []byte {
	dst = append(dst, "x'"...)
	dst = hex.AppendEncode(dst, src)
	dst = append(dst, '\'')
	return dst
}

// AppendInt64 appends an int64 value as decimal text.
func AppendInt64(buf []byte, value int64) []byte {
	return strconv.AppendInt(buf, value, 10)
}

// AppendUint64 appends a uint64 value as decimal text.
func AppendUint64(buf []byte, value uint64) []byte {
	return strconv.AppendUint(buf, value, 10)
}

// AppendFloat64 appends a float64 value, handling infinities.
func AppendFloat64(buf []byte, value float64, bitSize int) []byte {
	if !math.IsInf(value, 0) {
		return strconv.AppendFloat(buf, value, 'f', -1, bitSize)
	}
	if math.IsInf(value, 1) {
		return append(buf, "+Infinity"...)
	}
	return append(buf, "-Infinity"...)
}
