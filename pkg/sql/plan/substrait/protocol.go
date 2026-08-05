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
	"errors"
	"fmt"

	"google.golang.org/protobuf/encoding/protowire"
)

const (
	ProtocolVersion        = 1
	TaeReadProtocolVersion = 1
	CapabilityDocument     = `{"protocol_version":1,"substrait_version":"0.78.0","tae_read_protocol_version":1,"tae_read_feature_bits":0,"operators":["read","filter","project","aggregate","sort","fetch"],"types":["bool","i8","i16","i32","i64","fp32","fp64","string","date","varchar","precision_timestamp_us"],"scalar_functions":["and","or","not","equal","not_equal","lt","lte","gt","gte","is_null","is_not_null","is_not_distinct_from","add","subtract","multiply","divide","modulus","between"],"aggregate_functions":["count","sum","min","max","avg"],"transport":"arrow-flight","sirius_execution_contract":1,"max_plan_bytes":16777216}`
)

var CapabilityHash = sha256.Sum256([]byte(CapabilityDocument))

// TaeRead is the exact matrixone.sirius.v1.TaeRead v1 wire contract.
type TaeRead struct {
	ProtocolVersion                                          uint32
	FeatureBits                                              uint64
	ReadRef, QueryID                                         []byte
	AccountID, TableID                                       uint64
	SnapshotTS, SchemaDigest, ManifestSHA256, CapabilityHash []byte
	ExpiresAtUnixMS                                          uint64
}

func (r *TaeRead) Validate(nowUnixMS uint64) error {
	if r == nil || r.ProtocolVersion != TaeReadProtocolVersion || r.FeatureBits != 0 {
		return errors.New("invalid TaeRead protocol")
	}
	if len(r.ReadRef) != 32 || len(r.QueryID) == 0 || len(r.QueryID) > 4096 || len(r.SnapshotTS) != 12 || len(r.SchemaDigest) != sha256.Size || len(r.ManifestSHA256) != sha256.Size || len(r.CapabilityHash) != sha256.Size {
		return errors.New("invalid TaeRead identity or digest length")
	}
	if r.AccountID == 0 || r.TableID == 0 || r.ExpiresAtUnixMS <= nowUnixMS {
		return errors.New("invalid or expired TaeRead")
	}
	if !equalBytes(r.CapabilityHash, CapabilityHash[:]) {
		return errors.New("TaeRead capability mismatch")
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
	return b, nil
}

func UnmarshalTaeRead(b []byte, nowUnixMS uint64) (*TaeRead, error) {
	if len(b) == 0 || len(b) > 16<<10 {
		return nil, errors.New("invalid TaeRead size")
	}
	r := new(TaeRead)
	var seen uint16
	for len(b) != 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		b = b[n:]
		if num < 1 || num > 11 || seen&(1<<uint(num-1)) != 0 {
			return nil, errors.New("unknown or duplicate TaeRead field")
		}
		seen |= 1 << uint(num-1)
		integer := num == 1 || num == 2 || num == 5 || num == 6 || num == 11
		if integer && typ != protowire.VarintType || !integer && typ != protowire.BytesType {
			return nil, errors.New("wrong TaeRead wire type")
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
					return nil, errors.New("TaeRead version overflow")
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
	if seen != ((1<<11)-1)&^(1<<1) && seen != (1<<11)-1 {
		return nil, errors.New("missing TaeRead field")
	}
	if err := r.Validate(nowUnixMS); err != nil {
		return nil, err
	}
	return r, nil
}

type ResolveTaeReadRequest struct{ TaeRead, RequestedSchema []byte }
type ResolveTaeReadResponse struct{ TaeRead, Manifest, CanonicalSchema []byte }

func UnmarshalResolveRequest(b []byte) (ResolveTaeReadRequest, error) {
	fields, err := consumeStrictBytes(b, 2, 17<<20)
	if err != nil {
		return ResolveTaeReadRequest{}, err
	}
	if len(fields[0]) == 0 || len(fields[1]) == 0 || len(fields[1]) > 1<<20 {
		return ResolveTaeReadRequest{}, errors.New("invalid resolve request")
	}
	return ResolveTaeReadRequest{TaeRead: fields[0], RequestedSchema: fields[1]}, nil
}

func MarshalResolveResponse(r ResolveTaeReadResponse) ([]byte, error) {
	if len(r.TaeRead) == 0 || len(r.Manifest) == 0 || len(r.CanonicalSchema) == 0 || len(r.Manifest) > 64<<20 || len(r.CanonicalSchema) > 1<<20 {
		return nil, errors.New("invalid resolve response")
	}
	var b []byte
	b = appendBytes(b, 1, r.TaeRead)
	b = appendBytes(b, 2, r.Manifest)
	b = appendBytes(b, 3, r.CanonicalSchema)
	return b, nil
}

func consumeStrictBytes(b []byte, count protowire.Number, maximum int) ([][]byte, error) {
	if len(b) == 0 || len(b) > maximum {
		return nil, errors.New("invalid protobuf size")
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
			return nil, errors.New("unknown, duplicate, or mistyped protobuf field")
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
		return nil, fmt.Errorf("missing protobuf field")
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
