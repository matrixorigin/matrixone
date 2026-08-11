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

package substrait

import (
	"crypto/sha256"
	"io"
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"google.golang.org/protobuf/encoding/protowire"
)

const (
	ProtocolVersion        = 1
	TaeReadProtocolVersion = 1
	maxTaeReadSize         = 16 << 10
	maxResolveRequestSize  = maxTaeReadSize + maxCanonicalSchemaSize + 32
	// GC protection stores the expiry as Unix nanoseconds. Keep the wire value
	// within that signed range before any consumer converts it through
	// time.UnixMilli(...).UnixNano().
	maxTaeReadExpiryUnixMS = uint64(math.MaxInt64 / 1_000_000)
	CapabilityDocument     = `{"protocol_version":1,"substrait_version":"0.78.0","tae_read_protocol_version":1,"tae_read_feature_bits":0,"operators":["read","filter","project","aggregate","sort","fetch"],"types":["bool","i8","i16","i32","i64","fp32","fp64","fixed_char","varchar"],"semantic_registry":"exact-mo-overload-argument-result-nullability-v1","scalar_overloads":["and(bool,bool)->bool","or(bool,bool)->bool","not(bool)->bool","equal(i64,i64)->bool","not_equal(i64,i64)->bool","lt(i64,i64)->bool","lte(i64,i64)->bool","gt(i64,i64)->bool","gte(i64,i64)->bool","is_null(i64)->bool","is_not_null(i64)->bool","is_not_distinct_from(i64,i64)->bool","add(i64,i64)->i64","subtract(i64,i64)->i64","multiply(i64,i64)->i64","modulus(i64,i64)->i64","between(i64,i64,i64)->bool"],"aggregate_overloads":["count(i64)->i64","count_all(i64_literal)->i64","min(i64)->i64","max(i64)->i64"],"transport":"arrow-flight","sirius_execution_contract":1,"max_plan_bytes":16777216}`
)

var CapabilityHash = sha256.Sum256([]byte(CapabilityDocument))

// TaeRead is the exact matrixone.sirius.v1.TaeRead v1 wire contract.
type TaeRead struct {
	ProtocolVersion                                          uint32
	FeatureBits                                              uint64
	ReadRef, QueryID                                         []byte
	AccountID, DatabaseID, TableID                           uint64
	SnapshotTS, SchemaDigest, ManifestSHA256, CapabilityHash []byte
	// ExpiresAtUnixMS revokes new resolution. It is not a GC-retention
	// deadline; the execution owner releases the pin after Finish.
	ExpiresAtUnixMS uint64
}

func (r *TaeRead) Validate(nowUnixMS uint64) error {
	if r == nil || r.ProtocolVersion != TaeReadProtocolVersion || r.FeatureBits != 0 {
		return moerr.NewInternalErrorNoCtx("invalid TaeRead protocol")
	}
	if len(r.ReadRef) != 32 || len(r.QueryID) == 0 || len(r.QueryID) > 4096 || len(r.SnapshotTS) != 12 || len(r.SchemaDigest) != sha256.Size || len(r.ManifestSHA256) != sha256.Size || len(r.CapabilityHash) != sha256.Size {
		return moerr.NewInternalErrorNoCtx("invalid TaeRead identity or digest length")
	}
	if r.AccountID == 0 || r.DatabaseID == 0 || r.TableID == 0 || r.ExpiresAtUnixMS <= nowUnixMS || r.ExpiresAtUnixMS > maxTaeReadExpiryUnixMS {
		return moerr.NewInternalErrorNoCtx("invalid or expired TaeRead")
	}
	if !equalBytes(r.CapabilityHash, CapabilityHash[:]) {
		return moerr.NewInternalErrorNoCtx("TaeRead capability mismatch")
	}
	return nil
}

func MarshalTaeRead(r *TaeRead) ([]byte, error) {
	if err := r.Validate(0); err != nil {
		return nil, err
	}
	var b []byte
	b = appendUint(b, 1, uint64(r.ProtocolVersion))
	b = appendUint(b, 2, r.FeatureBits)
	b = appendBytes(b, 3, r.ReadRef)
	b = appendBytes(b, 4, r.QueryID)
	b = appendUint(b, 5, r.AccountID)
	b = appendUint(b, 6, r.TableID)
	b = appendBytes(b, 7, r.SnapshotTS)
	b = appendBytes(b, 8, r.SchemaDigest)
	b = appendBytes(b, 9, r.ManifestSHA256)
	b = appendBytes(b, 10, r.CapabilityHash)
	b = appendUint(b, 11, r.ExpiresAtUnixMS)
	b = appendUint(b, 12, r.DatabaseID)
	return b, nil
}

func UnmarshalTaeRead(b []byte, nowUnixMS uint64) (*TaeRead, error) {
	if len(b) == 0 || len(b) > maxTaeReadSize {
		return nil, moerr.NewInternalErrorNoCtx("invalid TaeRead size")
	}
	r := new(TaeRead)
	var seen uint16
	for len(b) != 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		b = b[n:]
		if num < 1 || num > 12 || seen&(1<<uint(num-1)) != 0 {
			return nil, moerr.NewInternalErrorNoCtx("unknown or duplicate TaeRead field")
		}
		seen |= 1 << uint(num-1)
		integer := num == 1 || num == 2 || num == 5 || num == 6 || num == 11 || num == 12
		if integer && typ != protowire.VarintType || !integer && typ != protowire.BytesType {
			return nil, moerr.NewInternalErrorNoCtx("wrong TaeRead wire type")
		}
		if integer {
			v, m := protowire.ConsumeVarint(b)
			if m < 0 {
				return nil, protowire.ParseError(m)
			}
			b = b[m:]
			switch num {
			case 1:
				if v > 1<<32-1 {
					return nil, moerr.NewInternalErrorNoCtx("TaeRead version overflow")
				}
				r.ProtocolVersion = uint32(v)
			case 2:
				r.FeatureBits = v
			case 5:
				r.AccountID = v
			case 6:
				r.TableID = v
			case 11:
				r.ExpiresAtUnixMS = v
			case 12:
				r.DatabaseID = v
			}
		} else {
			v, m := protowire.ConsumeBytes(b)
			if m < 0 {
				return nil, protowire.ParseError(m)
			}
			b = b[m:]
			v = append([]byte(nil), v...)
			switch num {
			case 3:
				r.ReadRef = v
			case 4:
				r.QueryID = v
			case 7:
				r.SnapshotTS = v
			case 8:
				r.SchemaDigest = v
			case 9:
				r.ManifestSHA256 = v
			case 10:
				r.CapabilityHash = v
			}
		}
	}
	// feature_bits is the only zero-valued field in v1, and canonical proto3
	// encoding omits it. Every identity and digest field remains mandatory.
	if seen != ((1<<12)-1)&^(1<<1) && seen != (1<<12)-1 {
		return nil, moerr.NewInternalErrorNoCtx("missing TaeRead field")
	}
	if err := r.Validate(nowUnixMS); err != nil {
		return nil, err
	}
	return r, nil
}

type ResolveTaeReadRequest struct{ TaeRead, RequestedSchema []byte }
type ResolveTaeReadResponse struct{ TaeRead, Manifest, CanonicalSchema []byte }

func UnmarshalResolveRequest(b []byte) (ResolveTaeReadRequest, error) {
	fields, err := consumeStrictBytes(b, 2, maxResolveRequestSize)
	if err != nil {
		return ResolveTaeReadRequest{}, err
	}
	if len(fields[0]) == 0 || len(fields[0]) > maxTaeReadSize || len(fields[1]) == 0 || len(fields[1]) > maxCanonicalSchemaSize {
		return ResolveTaeReadRequest{}, moerr.NewInternalErrorNoCtx("invalid resolve request")
	}
	return ResolveTaeReadRequest{TaeRead: fields[0], RequestedSchema: fields[1]}, nil
}

func MarshalResolveResponse(r ResolveTaeReadResponse) ([]byte, error) {
	size, err := resolveResponseSize(r)
	if err != nil {
		return nil, err
	}
	b := make([]byte, 0, size)
	b = appendBytes(b, 1, r.TaeRead)
	b = appendBytes(b, 2, r.Manifest)
	b = appendBytes(b, 3, r.CanonicalSchema)
	return b, nil
}

func resolveResponseSize(r ResolveTaeReadResponse) (int, error) {
	if len(r.TaeRead) == 0 || len(r.TaeRead) > maxTaeReadSize || len(r.Manifest) == 0 || len(r.CanonicalSchema) == 0 || len(r.Manifest) > maxManifestSize || len(r.CanonicalSchema) > maxCanonicalSchemaSize {
		return 0, moerr.NewInternalErrorNoCtx("invalid resolve response")
	}
	return protowire.SizeTag(1) + protowire.SizeBytes(len(r.TaeRead)) +
		protowire.SizeTag(2) + protowire.SizeBytes(len(r.Manifest)) +
		protowire.SizeTag(3) + protowire.SizeBytes(len(r.CanonicalSchema)), nil
}

// writeResolveResponse emits field framing and immutable payload slices
// directly, avoiding a second manifest-sized allocation in the HTTP path.
func writeResolveResponse(w io.Writer, r ResolveTaeReadResponse) error {
	if w == nil {
		return moerr.NewInternalErrorNoCtx("invalid resolve response writer")
	}
	if _, err := resolveResponseSize(r); err != nil {
		return err
	}
	for _, field := range []struct {
		number protowire.Number
		data   []byte
	}{{1, r.TaeRead}, {2, r.Manifest}, {3, r.CanonicalSchema}} {
		header := protowire.AppendTag(nil, field.number, protowire.BytesType)
		header = protowire.AppendVarint(header, uint64(len(field.data)))
		if err := writeFull(w, header); err != nil {
			return err
		}
		if err := writeFull(w, field.data); err != nil {
			return err
		}
	}
	return nil
}

func writeFull(w io.Writer, b []byte) error {
	for len(b) != 0 {
		n, err := w.Write(b)
		if n < 0 || n > len(b) {
			return io.ErrShortWrite
		}
		if n > 0 {
			b = b[n:]
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

func consumeStrictBytes(b []byte, count protowire.Number, maximum int) ([][]byte, error) {
	if len(b) == 0 || len(b) > maximum {
		return nil, moerr.NewInternalErrorNoCtx("invalid protobuf size")
	}
	result := make([][]byte, count)
	var seen uint64
	for len(b) != 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		b = b[n:]
		if num < 1 || num > count || typ != protowire.BytesType || seen&(1<<uint(num-1)) != 0 {
			return nil, moerr.NewInternalErrorNoCtx("unknown, duplicate, or mistyped protobuf field")
		}
		seen |= 1 << uint(num-1)
		v, m := protowire.ConsumeBytes(b)
		if m < 0 {
			return nil, protowire.ParseError(m)
		}
		b = b[m:]
		result[num-1] = append([]byte(nil), v...)
	}
	if seen != 1<<uint(count)-1 {
		return nil, moerr.NewInternalErrorNoCtxf("missing protobuf field")
	}
	return result, nil
}

func appendUint(b []byte, n protowire.Number, v uint64) []byte {
	if v == 0 {
		return b
	}
	b = protowire.AppendTag(b, n, protowire.VarintType)
	return protowire.AppendVarint(b, v)
}
func appendBytes(b []byte, n protowire.Number, v []byte) []byte {
	if len(v) == 0 {
		return b
	}
	b = protowire.AppendTag(b, n, protowire.BytesType)
	return protowire.AppendBytes(b, v)
}
func equalBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	var d byte
	for i := range a {
		d |= a[i] ^ b[i]
	}
	return d == 0
}
