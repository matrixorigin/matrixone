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
}

type legacyMissingColumnProof struct {
	ObjectName string
	Seqnum     uint16
}

type legacyMissingColumnKey struct{}

// WithM0ProvenObject marks metadata that has passed the complete M0 object
// proof. Metadata consumers may interpret an absent appended column as NULL
// only when this marker is present.
func WithM0ProvenObject(ctx context.Context, proof M0ObjectProof) context.Context {
	return context.WithValue(ctx, m0ProvenObjectKey{}, proof)
}

func IsM0ProvenObject(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	proof, ok := ctx.Value(m0ProvenObjectKey{}).(M0ObjectProof)
	return ok && proof.ObjectName != "" && proof.WrittenSize <= proof.SchemaSize
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
	if proof, ok := M0ProvenObject(ctx); ok &&
		proof.ObjectName == objectName &&
		uint32(seqnum) >= proof.WrittenSize &&
		uint32(seqnum) < proof.SchemaSize {
		return true
	}
	if ctx == nil {
		return false
	}
	proof, ok := ctx.Value(legacyMissingColumnKey{}).(legacyMissingColumnProof)
	return ok && proof.ObjectName == objectName && proof.Seqnum == seqnum
}
