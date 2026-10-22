// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package objectio

import (
	"context"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

var ErrMissingColumnWithoutProof error = moerr.NewInternalErrorNoCtx("objectio: missing normal column without proof")

// Preserve the sentinel identity while adding the rejected object's details.
type m0DetailError struct {
	cause  error
	detail string
}

func (e *m0DetailError) Error() string { return e.cause.Error() + ": " + e.detail }
func (e *m0DetailError) Unwrap() error { return e.cause }

func wrapM0Error(cause error, format string, args ...any) error {
	return &m0DetailError{cause: cause, detail: fmt.Sprintf(format, args...)}
}

type m0ProvenObjectKey struct{}

type M0ObjectProof struct {
	ObjectName  string
	WrittenSize uint32
	SchemaSize  uint32
	relationID  uint64
	version     uint32
	birth       *plan.AppendMissingState
}

type legacyMissingColumnProof struct {
	ObjectName string
	Seqnum     uint16
}

type legacyMissingColumnKey struct{}

// WithM0ProvenObject marks metadata that has passed the complete M0 object
// proof. Missing suffixes use the immutable birth descriptor, if present;
// this marker never authorizes interpreting a hole in the written prefix.
func WithM0ProvenObject(ctx context.Context, proof M0ObjectProof) context.Context {
	return context.WithValue(ctx, m0ProvenObjectKey{}, proof)
}

func IsM0ProvenObject(ctx context.Context) bool {
	_, ok := M0ProvenObject(ctx)
	return ok
}

func M0ProvenObject(ctx context.Context) (M0ObjectProof, bool) {
	if ctx == nil {
		return M0ObjectProof{}, false
	}
	proof, ok := ctx.Value(m0ProvenObjectKey{}).(M0ObjectProof)
	if !ok || proof.ObjectName == "" || proof.WrittenSize > proof.SchemaSize {
		return M0ObjectProof{}, false
	}
	return proof, true
}

func WithLegacyMissingColumnProof(ctx context.Context, objectName string, seqnum uint16) context.Context {
	return context.WithValue(ctx, legacyMissingColumnKey{}, legacyMissingColumnProof{
		ObjectName: objectName,
		Seqnum:     seqnum,
	})
}

func CanFillMissingColumn(ctx context.Context, objectName string, seqnum uint16) bool {
	_, ok := MissingColumnFill(ctx, objectName, seqnum)
	return ok
}

// MissingColumnFill returns an explicit kind only for a proved absent suffix.
// A physically present NULL is read from its bitmap and never enters here.
func MissingColumnFill(ctx context.Context, objectName string, seqnum uint16) (plan.MissingFillKind, bool) {
	if proof, ok := M0ProvenObject(ctx); ok &&
		proof.ObjectName == objectName &&
		uint32(seqnum) >= proof.WrittenSize &&
		uint32(seqnum) < proof.SchemaSize {
		if proof.birth == nil {
			return plan.MissingFillKind_MISSING_FILL_NULL_V1, true
		}
		if uint32(seqnum) < proof.birth.BaseWidth {
			return plan.MissingFillKind_MISSING_FILL_INVALID, false
		}
		col := proof.birth.Columns[uint32(seqnum)-proof.birth.BaseWidth]
		return col.Kind, true
	}
	if ctx == nil {
		return plan.MissingFillKind_MISSING_FILL_INVALID, false
	}
	proof, ok := ctx.Value(legacyMissingColumnKey{}).(legacyMissingColumnProof)
	if ok && proof.ObjectName == objectName && proof.Seqnum == seqnum {
		return plan.MissingFillKind_MISSING_FILL_NULL_V1, true
	}
	return plan.MissingFillKind_MISSING_FILL_INVALID, false
}
