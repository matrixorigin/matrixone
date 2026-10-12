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
	"encoding/binary"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	spb "github.com/substrait-io/substrait-protobuf/go/substraitpb"
	"github.com/substrait-io/substrait-protobuf/go/substraitpb/extensions"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/types/known/anypb"
)

const (
	// Mask value, not a bit index: this is the append-only Sirius ABI-v1 flag.
	MOExactDecimalV1Capability uint64 = 16
	MOExactDecimalV1URI               = "urn:matrixone:sirius:exact-decimal:v1"
	exactDecimalLiteralURL            = "type.googleapis.com/matrixone.sirius.numeric.v1.ExactDecimalLiteral"
	exactDecimalTypeAnchor     uint32 = 1
	exactDecimalURNAnchor      uint32 = 1
)

// EmbeddedExportProfile is copied into a Candidate before validation. Its
// private field cannot be changed by a caller between validation and Build.
type EmbeddedExportProfile struct{ exactDecimalV1 bool }

func NewEmbeddedExportProfile(capabilities uint64) EmbeddedExportProfile {
	return EmbeddedExportProfile{exactDecimalV1: capabilities&MOExactDecimalV1Capability != 0}
}

func (p EmbeddedExportProfile) Name() string {
	if p.exactDecimalV1 {
		return "mo-exact-decimal-v1"
	}
	return "legacy"
}

func (c *Candidate) NumericProfile() string {
	if c == nil {
		return "legacy"
	}
	return c.profile.Name()
}

func exactDecimalBits(t *planpb.Type) int {
	if t == nil {
		return 0
	}
	switch types.T(t.Id) {
	case types.T_decimal64:
		return 64
	case types.T_decimal128:
		return 128
	case types.T_decimal256:
		return 256
	default:
		return 0
	}
}

func (p EmbeddedExportProfile) substraitType(t *planpb.Type) (*spb.Type, error) {
	bits := exactDecimalBits(t)
	if !p.exactDecimalV1 || bits == 0 {
		return substraitType(t)
	}
	maximum := int32(18)
	if bits == 128 {
		maximum = 38
	} else if bits == 256 {
		maximum = 76
	}
	if t.Width < 1 || t.Width > maximum || t.Scale < 0 || t.Scale > t.Width {
		return nil, notEligiblef(EligibilityType, "invalid MO exact decimal descriptor (%d,%d,%d)", bits, t.Width, t.Scale)
	}
	nullable := spb.Type_NULLABILITY_NULLABLE
	if t.NotNullable {
		nullable = spb.Type_NULLABILITY_REQUIRED
	}
	return &spb.Type{Kind: &spb.Type_UserDefined_{UserDefined: &spb.Type_UserDefined{
		TypeReference: exactDecimalTypeAnchor, Nullability: nullable,
		TypeParameters: []*spb.Type_Parameter{
			{Parameter: &spb.Type_Parameter_Integer{Integer: int64(bits)}},
			{Parameter: &spb.Type_Parameter_Integer{Integer: int64(t.Width)}},
			{Parameter: &spb.Type_Parameter_Integer{Integer: int64(t.Scale)}},
		},
	}}}, nil
}

func (e *exporter) substraitType(t *planpb.Type) (*spb.Type, error) {
	result, err := e.profile.substraitType(t)
	if err == nil && result.GetUserDefined() != nil {
		e.exactUsed = true
	}
	return result, err
}

func (e *exporter) namedStruct(t *planpb.TableDef) (*spb.NamedStruct, error) {
	return namedStructForProfile(t, e.profile)
}

func (e *exporter) extensionURNs() []*extensions.SimpleExtensionURN {
	if !e.exactUsed {
		return nil
	}
	return []*extensions.SimpleExtensionURN{{ExtensionUrnAnchor: exactDecimalURNAnchor, Urn: MOExactDecimalV1URI}}
}

func (e *exporter) literal(l *planpb.Literal, typ *planpb.Type) (*spb.Expression, error) {
	if !e.profile.exactDecimalV1 || exactDecimalBits(typ) == 0 {
		return literal(l, typ)
	}
	declared, err := e.substraitType(typ)
	if err != nil {
		return nil, err
	}
	if l == nil {
		return nil, moerr.NewInternalErrorNoCtx("substrait: missing exact decimal literal")
	}
	if l.Isnull {
		if typ.NotNullable {
			return nil, moerr.NewInternalErrorNoCtx("substrait: NULL exact literal has a required type")
		}
		return &spb.Expression{RexType: &spb.Expression_Literal_{Literal: &spb.Expression_Literal{
			LiteralType: &spb.Expression_Literal_Null{Null: declared},
		}}}, nil
	}
	coefficient := make([]byte, exactDecimalBits(typ)/8)
	switch value := l.Value.(type) {
	case *planpb.Literal_Decimal64Val:
		if exactDecimalBits(typ) != 64 || value.Decimal64Val == nil {
			return nil, moerr.NewInternalErrorNoCtx("substrait: exact Decimal64 literal carrier mismatch")
		}
		binary.LittleEndian.PutUint64(coefficient, uint64(value.Decimal64Val.A))
	case *planpb.Literal_Decimal128Val:
		if exactDecimalBits(typ) != 128 || value.Decimal128Val == nil {
			return nil, moerr.NewInternalErrorNoCtx("substrait: exact Decimal128 literal carrier mismatch")
		}
		binary.LittleEndian.PutUint64(coefficient, uint64(value.Decimal128Val.A))
		binary.LittleEndian.PutUint64(coefficient[8:], uint64(value.Decimal128Val.B))
	default:
		return nil, moerr.NewInternalErrorNoCtx("substrait: unsupported exact decimal literal carrier")
	}
	return exactCoefficientLiteral(coefficient, declared, !typ.NotNullable), nil
}

func exactCoefficientLiteral(coefficient []byte, declared *spb.Type, nullable bool) *spb.Expression {
	// Canonical SDK ExactDecimalLiteral: bytes coefficient_le = 1. Encoding
	// through protowire preserves the protobuf contract without a second schema.
	payload := protowire.AppendTag(nil, 1, protowire.BytesType)
	payload = protowire.AppendBytes(payload, coefficient)
	return &spb.Expression{RexType: &spb.Expression_Literal_{Literal: &spb.Expression_Literal{
		Nullable: nullable,
		LiteralType: &spb.Expression_Literal_UserDefined_{UserDefined: &spb.Expression_Literal_UserDefined{
			TypeAnchorType: &spb.Expression_Literal_UserDefined_TypeReference{TypeReference: exactDecimalTypeAnchor},
			TypeParameters: declared.GetUserDefined().TypeParameters,
			Val:            &spb.Expression_Literal_UserDefined_Value{Value: &anypb.Any{TypeUrl: exactDecimalLiteralURL, Value: payload}},
		}},
	}}}
}
