// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"reflect"
	"strings"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

func isPlanMySQLStringType(id int32) bool {
	switch id {
	case 60, 61, 64, 65, 70, 71:
		return true
	default:
		return false
	}
}

// ValidateStringLiteralForms rejects unknown wire enum values at a plan owner
// boundary. Protobuf intentionally preserves unknown enum integers, so callers
// must validate a decoded expression before treating its literal provenance as
// executable state.
func (m *Expr) ValidateStringLiteralForms() error {
	return m.walkStringLiterals(func(expr *Expr, lit *Literal) error {
		return expr.validateStringLiteralForm(lit)
	})
}

func (m *Expr) validateOwnStringLiteralForm() error {
	if m == nil || m.GetLit() == nil {
		return nil
	}
	return m.validateStringLiteralForm(m.GetLit())
}

func (m *Expr) validateStringLiteralForm(lit *Literal) error {
	if lit.LiteralForm < StringLiteralForm_STRING_LITERAL_NONE ||
		lit.LiteralForm > StringLiteralForm_STRING_LITERAL_BIT {
		return moerr.NewInvalidInputNoCtxf("invalid string literal form %d", lit.LiteralForm)
	}
	if lit.LiteralForm == StringLiteralForm_STRING_LITERAL_NONE {
		return nil
	}
	if lit.Isnull || lit.Value == nil {
		return moerr.NewInvalidInputNoCtx("string literal form requires a non-NULL literal value")
	}
	if _, ok := lit.Value.(*Literal_Sval); !ok || !isPlanMySQLStringType(m.Typ.Id) {
		return moerr.NewInvalidInputNoCtx("string literal form requires a string literal and string type")
	}
	binarySyntax := lit.LiteralForm == StringLiteralForm_STRING_LITERAL_HEX ||
		lit.LiteralForm == StringLiteralForm_STRING_LITERAL_BIT
	if lit.IsBin != binarySyntax {
		return moerr.NewInvalidInputNoCtx("string literal form and isBin disagree")
	}
	return nil
}

// NormalizeTextLiteralFormsForCompatibility maps the explicit ordinary TEXT
// spelling to the zero value used by older serialized plans. Semantic forms
// such as HEX, BIT, and BINARY_INTRODUCER remain distinct.
func (m *Expr) NormalizeTextLiteralFormsForCompatibility() error {
	if err := m.ValidateStringLiteralForms(); err != nil {
		return err
	}
	return m.walkStringLiterals(func(expr *Expr, lit *Literal) error {
		if lit.LiteralForm == StringLiteralForm_STRING_LITERAL_TEXT &&
			staticStringDomainForPlanType(expr.Typ) == planStringDomainText {
			lit.LiteralForm = StringLiteralForm_STRING_LITERAL_NONE
		}
		return nil
	})
}

const (
	planStringDomainNone uint8 = iota
	planStringDomainText
	planStringDomainBinary
)

func staticStringDomainForPlanType(typ Type) uint8 {
	if !isPlanMySQLStringType(typ.Id) {
		return planStringDomainNone
	}
	// CharsetBinary is 1 in the plan wire contract and is authoritative even
	// for CHAR/VARCHAR/TEXT-shaped OIDs.
	if typ.Charset == 1 {
		return planStringDomainBinary
	}
	switch typ.Id {
	case 64, 65, 70: // BINARY, VARBINARY, BLOB
		return planStringDomainBinary
	default:
		return planStringDomainText
	}
}

const (
	possibleStringDomainText uint8 = 1 << iota
	possibleStringDomainBinary
)

func possibleStringDomainForStaticType(typ Type) uint8 {
	switch staticStringDomainForPlanType(typ) {
	case planStringDomainText:
		return possibleStringDomainText
	case planStringDomainBinary:
		return possibleStringDomainBinary
	default:
		return 0
	}
}

// RequiresMORPCVersion23StringProvenance reports whether an owner can produce
// runtime string provenance that differs from an expression's static domain.
// Besides cross-domain literals, IF/CASE/COALESCE preserve the domain of their
// selected value through binder-inserted casts. Older workers cannot represent
// that dynamic provenance, so such plans cannot cross a remote owner boundary
// before MORPC version 23.
func RequiresMORPCVersion23StringProvenance(owner any) (bool, error) {
	required := false
	err := walkExpressionsInOwner(owner, func(expr *Expr) error {
		_, exprRequired, err := expr.possibleRuntimeStringDomains()
		required = required || exprRequired
		return err
	})
	return required, err
}

// RequiresMORPCVersion23StringLiterals is retained for callers built against
// the original literal-only API. Its result now includes dynamic provenance.
func RequiresMORPCVersion23StringLiterals(owner any) (bool, error) {
	return RequiresMORPCVersion23StringProvenance(owner)
}

// RequiresMORPCVersion30NumericPrefix reports whether an owner contains a
// planner-injected CAST that uses the numeric-prefix sentinel.
func RequiresMORPCVersion30NumericPrefix(owner any) (bool, error) {
	features, err := RequiredRemoteExpressionFeatures(owner)
	return features.NumericPrefix, err
}

// RequiresMORPCVersion36JSONComparisonParam reports whether an owner contains
// the internal prepared-JSON comparison function.  The function is deliberately
// identified by its numeric ID: unlike ordinary SQL functions, its name is an
// implementation detail and the receiver dispatches it by ID after decoding.
func RequiresMORPCVersion36JSONComparisonParam(owner any) (bool, error) {
	features, err := RequiredRemoteExpressionFeatures(owner)
	return features.JSONComparisonParam, err
}

// RequiresMORPCVersion36MixedJSONBooleanEquality reports whether an owner
// contains an equality operation whose physical operands are JSON and BOOL.
// Pre-v36 workers dispatch such plans through varlena comparison overloads and
// cannot safely execute the BOOL vector.
func RequiresMORPCVersion36MixedJSONBooleanEquality(owner any) (bool, error) {
	features, err := RequiredRemoteExpressionFeatures(owner)
	return features.MixedJSONBooleanEquality, err
}

// RequiresMORPCVersion59NumericFormatArguments reports whether an owner
// contains FORMAT with a physical numeric first argument. The v59 fence is
// needed even when the function keeps overload IDs 0/1: those IDs were
// historically string-only on older receivers.
func RequiresMORPCVersion59NumericFormatArguments(owner any) (bool, error) {
	features, err := RequiredRemoteExpressionFeatures(owner)
	return features.FormatNumericArguments, err
}

// RequiresMORPCVersion72IPFunctionSemantics reports whether an owner contains
// an IP function whose serialized execution contract changed in MORPC v72.
func RequiresMORPCVersion72IPFunctionSemantics(owner any) (bool, error) {
	features, err := RequiredRemoteExpressionFeatures(owner)
	return features.IPFunctionSemantics, err
}

// RequiresMORPCVersion80StringNumericResultContracts reports whether an owner
// contains one of the corrected fixed-width string numeric result contracts.
func RequiresMORPCVersion80StringNumericResultContracts(owner any) (bool, error) {
	features, err := RequiredRemoteExpressionFeatures(owner)
	return features.StringNumericResultContracts, err
}

// RequiresMORPCVersion83BoundedConditionalStringDomains reports whether an
// owner contains a conditional string overload introduced with the bounded
// CHAR/VARCHAR and BINARY/VARBINARY result-domain contract.
func RequiresMORPCVersion83BoundedConditionalStringDomains(owner any) (bool, error) {
	features, err := RequiredRemoteExpressionFeatures(owner)
	return features.BoundedConditionalStringDomains, err
}

// RequiresMORPCVersion86ExpressionResultContracts reports whether an owner
// contains a follow-up expression contract that changes a result domain or
// overload identity.
func RequiresMORPCVersion86ExpressionResultContracts(owner any) (bool, error) {
	features, err := RequiredRemoteExpressionFeatures(owner)
	return features.ExpressionResultMetadataContracts ||
		features.TOBase64ResultContracts || features.IPFunctionResultContracts, err
}

// RequiresMORPCVersion85ExpressionResultContracts is retained for source
// compatibility with callers introduced before MORPC v85 was reserved for
// integer-parameter coercion. The actual admission epoch is v86.
// Deprecated: use RequiresMORPCVersion86ExpressionResultContracts.
func RequiresMORPCVersion85ExpressionResultContracts(owner any) (bool, error) {
	return RequiresMORPCVersion86ExpressionResultContracts(owner)
}

// RequiresMORPCVersion89DecimalLiteralSemantics reports whether an owner
// contains a plain decimal literal whose exact normalized binding must not be
// replayed by a pre-v89 binder.
func RequiresMORPCVersion89DecimalLiteralSemantics(owner any) (bool, error) {
	features, err := RequiredRemoteExpressionFeatures(owner)
	return features.DecimalLiteralSemantics, err
}

// RequiresMORPCVersion88DecimalLiteralSemantics is retained as a source-level
// compatibility alias. MORPC v88 is owned by canonical vector HLL keys.
// Deprecated: use RequiresMORPCVersion89DecimalLiteralSemantics.
func RequiresMORPCVersion88DecimalLiteralSemantics(owner any) (bool, error) {
	return RequiresMORPCVersion89DecimalLiteralSemantics(owner)
}

// RequiresMORPCVersion87DecimalLiteralSemantics is retained as a source-level
// compatibility alias. Decimal literal admission moved to v89 because v87 and
// v88 are owned by grouping provenance and canonical vector HLL keys.
// Deprecated: use RequiresMORPCVersion89DecimalLiteralSemantics.
func RequiresMORPCVersion87DecimalLiteralSemantics(owner any) (bool, error) {
	return RequiresMORPCVersion89DecimalLiteralSemantics(owner)
}

// RequiresMORPCVersion84DecimalLiteralSemantics is retained as a source-level
// compatibility alias. Decimal literal admission moved to v89 because v84 is
// already owned by the extended discrete percentile contract on main.
// Deprecated: use RequiresMORPCVersion89DecimalLiteralSemantics.
func RequiresMORPCVersion84DecimalLiteralSemantics(owner any) (bool, error) {
	return RequiresMORPCVersion89DecimalLiteralSemantics(owner)
}

// RequiresMORPCVersion82DecimalLiteralSemantics is retained as a source-level
// compatibility alias for callers introduced with the original decimal marker.
// Deprecated: use RequiresMORPCVersion89DecimalLiteralSemantics.
func RequiresMORPCVersion82DecimalLiteralSemantics(owner any) (bool, error) {
	return RequiresMORPCVersion89DecimalLiteralSemantics(owner)
}

// RequiresMORPCVersion90SpatialDistanceSemantics reports whether an owner
// contains a spatial-distance expression whose meaning or overload contract
// changed in MORPC v90. Constant folding must retain such an expression until
// persisted-expression admission has observed this requirement.
func RequiresMORPCVersion90SpatialDistanceSemantics(owner any) (bool, error) {
	features, err := RequiredRemoteExpressionFeatures(owner)
	return features.SpatialDistanceSemantics, err
}

// RequiresMORPCVersion89SpatialDistanceSemantics retains the pre-main source alias.
// Deprecated: use RequiresMORPCVersion90SpatialDistanceSemantics; admission is v90.
func RequiresMORPCVersion89SpatialDistanceSemantics(owner any) (bool, error) {
	return RequiresMORPCVersion90SpatialDistanceSemantics(owner)
}

// RequiresMORPCVersion86SpatialDistanceSemantics retains the original source alias.
// Deprecated: use RequiresMORPCVersion90SpatialDistanceSemantics; admission is v90.
func RequiresMORPCVersion86SpatialDistanceSemantics(owner any) (bool, error) {
	return RequiresMORPCVersion90SpatialDistanceSemantics(owner)
}

// RequiresMORPCVersion87SpatialDistanceSemantics retains the unmerged stack alias.
// Deprecated: use RequiresMORPCVersion90SpatialDistanceSemantics; admission is v90.
func RequiresMORPCVersion87SpatialDistanceSemantics(owner any) (bool, error) {
	return RequiresMORPCVersion90SpatialDistanceSemantics(owner)
}

// RequiresMORPCVersion88SpatialDistanceSemantics retains the pre-main source alias.
// Deprecated: use RequiresMORPCVersion90SpatialDistanceSemantics; admission is v90.
func RequiresMORPCVersion88SpatialDistanceSemantics(owner any) (bool, error) {
	return RequiresMORPCVersion90SpatialDistanceSemantics(owner)
}

const (
	equalFunctionID                  int32 = 0
	notEqualFunctionID               int32 = 1
	nullSafeEqualFunctionID          int32 = 406
	internalJSONComparisonFunctionID int32 = 577
	planBooleanTypeID                int32 = 10
	planJSONTypeID                   int32 = 62
	binFunctionID                    int32 = 270
	convFunctionID                   int32 = 367
	asciiFunctionID                  int32 = 52
	asciiInt32ResultTypeID           int32 = 22
	findInSetFunctionID              int32 = 101
	lengthUTF8FunctionID             int32 = 125
	strCmpFunctionID                 int32 = 344
	uncompressedLengthFunctionID     int32 = 389
	crc32FunctionID                  int32 = 81
	coalesceFunctionID               int32 = 74
	caseFunctionID                   int32 = 71
	iffFunctionID                    int32 = 113
	leftFunctionID                   int32 = 123
	rightFunctionID                  int32 = 166
	substringFunctionID              int32 = 210
	stringNumericInt32ResultTypeID   int32 = 22
	stringNumericInt64ResultTypeID   int32 = 23
	stringNumericUint64ResultTypeID  int32 = 28
	planVarcharTypeID                int32 = 61
	planBinaryTypeID                 int32 = 64
	planVarbinaryTypeID              int32 = 65
	planBlobTypeID                   int32 = 70
	planTextTypeID                   int32 = 71
	ipInt32ResultTypeID              int32 = 22
	planCharTypeID                   int32 = 60
	planDateTypeID                   int32 = 50
	planTimeTypeID                   int32 = 51
	planDatetimeTypeID               int32 = 52
	planTimestampTypeID              int32 = 53
	planInt64TypeID                  int32 = 23
	planUint32TypeID                 int32 = 27
	planAnyTypeID                    int32 = 0
	maxVarcharWidth                  int32 = 65535
)

// RemoteExpressionFeatures is the complete set of versioned expression
// capabilities that can make a pipeline unsafe on an older remote worker.
// NumericPrefix requires MORPC v30. JSONComparisonParam and
// MixedJSONBooleanEquality require MORPC v36. FormatNumericArguments requires
// MORPC v59. TypedConversionFunctions requires MORPC v64 because BIN/CONV
// overload identities and their fixed-width execution contracts changed in
// the same release. ASCIIInt32Result requires MORPC v65 because ASCII keeps
// its overload IDs but changes its physical result vector from UINT8 to INT32.
// A struct makes compatibility call sites name every capability instead of
// relying on positional booleans.
// RowDependentConvBases requires MORPC v69 for nonconstant or unsigned bases.
// IPFunctionSemantics requires MORPC v72 because the IP functions change
// existing overload semantics and add numeric INET_NTOA overloads.
// StringNumericResultContracts requires MORPC v80 because the listed string
// numeric functions keep overload IDs while changing their physical result
// vectors to signed INT/ BIGINT or BIGINT UNSIGNED.
// BoundedConditionalStringDomains requires MORPC v83 because the bounded
// BINARY/VARBINARY COALESCE overload identities are new to the registry.
// DecimalLiteralSemantics requires MORPC v89 because plain DECIMAL256
// literals are normalized and kept exact by the new planner, while older
// binders can round or reject the same persisted SQL at the Decimal128
// boundary.
// TOBase64ResultContracts and IPFunctionResultContracts require MORPC v86:
// the former changes a VARCHAR result bound and adds binary overloads, while
// the latter changes IP predicate results to INT32 and adds domain-aware
// INET_NTOA overloads.
// ExpressionResultMetadataContracts also requires MORPC v86 because bounded
// character slicing and fractional temporal conditional results change the
// serialized result metadata consumed by persisted views and remote workers.
// SpatialDistanceSemantics requires MORPC v90 because geodetic
// ST_FRECHETDISTANCE/ST_HAUSDORFFDISTANCE change the meaning of existing
// overloads and the distance family adds length-unit overloads.
// PreparedPrecisionScalar requires MORPC v95 because older executors lose
// scalar identity when CEIL/FLOOR precision passes through private CAST 5/6.
// DecimalDivisionSemantics requires MORPC v97 for new plans because older
// executors derive the quotient scale from the left operand instead of the
// result type. Legacy plans remain executable by v97 receivers.
type RemoteExpressionFeatures struct {
	NumericPrefix                   bool
	JSONComparisonParam             bool
	MixedJSONBooleanEquality        bool
	FormatNumericArguments          bool
	TypedConversionFunctions        bool
	IntegerArithmeticDomains        bool
	RowDependentConvBases           bool
	ASCIIInt32Result                bool
	StringNumericResultContracts    bool
	BoundedConditionalStringDomains bool
	IPFunctionSemantics             bool
	// IntegerParameterCoercion requires v85 for private CAST 5..8.
	IntegerParameterCoercion          bool
	TOBase64ResultContracts           bool
	IPFunctionResultContracts         bool
	ExpressionResultMetadataContracts bool
	DecimalLiteralSemantics           bool
	SpatialDistanceSemantics          bool
	PreparedPrecisionScalar           bool
	DecimalDivisionSemantics          bool
	TemporalResultContracts           bool
	InvalidTemporalResultContract     bool
	NormalizedIntervalUnits           bool
	LegacyIntervalUnits               bool
	WeekSessionDefault                bool
}

func (features RemoteExpressionFeatures) Any() bool {
	return features.NumericPrefix ||
		features.JSONComparisonParam ||
		features.MixedJSONBooleanEquality ||
		features.FormatNumericArguments ||
		features.TypedConversionFunctions ||
		features.ASCIIInt32Result ||
		features.IntegerArithmeticDomains ||
		features.RowDependentConvBases ||
		features.StringNumericResultContracts ||
		features.BoundedConditionalStringDomains ||
		features.IPFunctionSemantics ||
		features.IntegerParameterCoercion ||
		features.TOBase64ResultContracts ||
		features.IPFunctionResultContracts ||
		features.ExpressionResultMetadataContracts ||
		features.DecimalLiteralSemantics ||
		features.SpatialDistanceSemantics ||
		features.PreparedPrecisionScalar ||
		features.DecimalDivisionSemantics ||
		features.TemporalResultContracts ||
		features.InvalidTemporalResultContract ||
		features.NormalizedIntervalUnits ||
		features.LegacyIntervalUnits ||
		features.WeekSessionDefault
}

func hasPrivateIntegerPrecisionCast(expr *Expr) bool {
	if expr == nil {
		return false
	}
	fn := expr.GetF()
	if fn == nil || fn.Func == nil {
		return false
	}
	if int32(fn.Func.Obj>>32) == 21 {
		overload := int32(fn.Func.Obj)
		if overload == 5 || overload == 6 {
			return true
		}
	}
	for _, arg := range fn.Args {
		if hasPrivateIntegerPrecisionCast(arg) {
			return true
		}
	}
	return false
}

func isBoundedConditionalStringDomain(fn *Function) bool {
	if fn == nil || fn.Func == nil {
		return false
	}
	functionID := int32(fn.Func.Obj >> 32)
	overloadID := int32(fn.Func.Obj)
	return functionID == coalesceFunctionID && (overloadID == 30 || overloadID == 31)
}

// These IDs are kept numeric deliberately: pkg/pb/plan cannot import the
// planner's function package without creating an import cycle. Every listed
// function either changed the interpretation of an existing overload or
// gained overloads in the IP-function compatibility fix. The remote fence is
// therefore based on function identity, not on the operand types selected by a
// particular planner invocation.
const (
	remoteIPInet6AtonFunctionID       int32 = 392
	remoteIPInet6NtoaFunctionID       int32 = 393
	remoteIPInetAtonFunctionID        int32 = 394
	remoteIPInetNtoaFunctionID        int32 = 395
	remoteIPIsIPv4FunctionID          int32 = 396
	remoteIPIsIPv6FunctionID          int32 = 397
	remoteIPIsIPv4CompatFunctionID    int32 = 398
	remoteIPIsIPv4MappedFunctionID    int32 = 399
	remoteTOBase64FunctionID          int32 = 213
	remoteINETNTOAFunctionID          int32 = 395
	remoteSpatialDistanceFunctionID   int32 = 421
	remoteFrechetDistanceFunctionID   int32 = 506
	remoteHausdorffDistanceFunctionID int32 = 507
)

func isValidIntegerArgumentSource(id, source int32) bool {
	standard := source == 0 || source == 10 || isPlanNumericType(source) ||
		source == 55 || source == 66 || isPlanMySQLStringType(source)
	if id == 7 {
		return source == 0 || isPlanMySQLStringType(source)
	}
	if id == 8 {
		return standard || (source >= 50 && source <= 53) || source == 63
	}
	return standard
}

func validateIntegerArgumentCast(expr *Expr, overload int32) error {
	fn := expr.GetF()
	if fn == nil || len(fn.Args) != 2 || fn.Args[0] == nil || fn.Args[1] == nil {
		return moerr.NewNotSupportedNoCtx("invalid private integer parameter CAST arity")
	}
	target := int32(23)
	if overload == 7 || (overload != 8 && expr.Typ.Id == 28) {
		target = 28
	}
	if !isValidIntegerArgumentSource(overload, fn.Args[0].Typ.Id) {
		return moerr.NewNotSupportedNoCtx("invalid private integer parameter CAST source type")
	}
	if fn.Args[1].GetT() == nil || fn.Args[1].Typ.Id != target {
		return moerr.NewNotSupportedNoCtx("invalid private integer parameter CAST target marker")
	}
	if expr.Typ.Id != target {
		return moerr.NewNotSupportedNoCtx("invalid private integer parameter CAST result type")
	}
	return nil
}

func isRemoteIPFunction(functionID int32) bool {
	switch functionID {
	case remoteIPInet6AtonFunctionID,
		remoteIPInet6NtoaFunctionID,
		remoteIPInetAtonFunctionID,
		remoteIPInetNtoaFunctionID,
		remoteIPIsIPv4FunctionID,
		remoteIPIsIPv6FunctionID,
		remoteIPIsIPv4CompatFunctionID,
		remoteIPIsIPv4MappedFunctionID:
		return true
	default:
		return false
	}
}

func isTOBase64ResultContract(expr *Expr) bool {
	if expr == nil || expr.Typ.Id == 0 {
		return false
	}
	fn := expr.GetF()
	if fn == nil || fn.Func == nil || int32(fn.Func.Obj>>32) != remoteTOBase64FunctionID {
		return false
	}
	overloadID := int32(fn.Func.Obj)
	return overloadID >= 3 || (overloadID == 0 && expr.Typ.Id == planVarcharTypeID)
}

func isIPFunctionResultContract(expr *Expr) bool {
	if expr == nil {
		return false
	}
	fn := expr.GetF()
	if fn == nil || fn.Func == nil {
		return false
	}
	functionID := int32(fn.Func.Obj >> 32)
	overloadID := int32(fn.Func.Obj)
	if functionID == remoteINETNTOAFunctionID {
		return overloadID >= 9
	}
	if functionID == remoteIPIsIPv4FunctionID ||
		functionID == remoteIPIsIPv6FunctionID ||
		functionID == remoteIPIsIPv4CompatFunctionID ||
		functionID == remoteIPIsIPv4MappedFunctionID {
		return expr.Typ.Id == ipInt32ResultTypeID
	}
	return false
}

func isPlanTemporalType(id int32) bool {
	switch id {
	case planDateTypeID, planTimeTypeID, planDatetimeTypeID, planTimestampTypeID:
		return true
	default:
		return false
	}
}

func isKnownIntegerLiteral(expr *Expr) bool {
	if expr == nil {
		return false
	}
	if lit := expr.GetLit(); lit != nil {
		if lit.Isnull {
			return false
		}
		switch lit.Value.(type) {
		case *Literal_I8Val, *Literal_I16Val, *Literal_I32Val, *Literal_I64Val,
			*Literal_U8Val, *Literal_U16Val, *Literal_U32Val, *Literal_U64Val:
			return true
		default:
			return false
		}
	}
	fn := expr.GetF()
	if fn == nil || fn.Func == nil || fn.Func.GetObjName() != "cast" ||
		fn.GetSyntaxExplicitCast() || len(fn.Args) != 2 {
		return false
	}
	return isKnownIntegerLiteral(fn.Args[0])
}

// expressionCharacterWidth recovers the source width through an implicit
// planner cast. A new bounded slice commonly stores its source as an implicit
// VARCHAR(n) cast, while an explicit CAST is a user-requested semantic
// boundary and must not be treated as evidence that the outer slice changed.
func expressionCharacterWidth(expr *Expr) (int32, bool) {
	if expr == nil {
		return 0, false
	}
	fn := expr.GetF()
	if fn != nil && fn.Func != nil && fn.Func.GetObjName() == "cast" &&
		!fn.GetSyntaxExplicitCast() && len(fn.Args) > 0 {
		return expressionCharacterWidth(fn.Args[0])
	}
	if isPlanStringType(expr.Typ.Id) {
		maxWidth := maxVarcharWidth
		if isPlanBinaryType(expr.Typ.Id) {
			maxWidth = maxVarbinaryWidth
		}
		if expr.Typ.Width > 0 && expr.Typ.Width != maxWidth {
			return expr.Typ.Width, true
		}
	}
	if lit := expr.GetLit(); lit != nil && !lit.Isnull {
		if value, ok := lit.Value.(*Literal_Sval); ok {
			if isPlanBinaryType(expr.Typ.Id) {
				return int32(len(value.Sval)), true
			}
			return int32(utf8.RuneCountInString(value.Sval)), true
		}
	}
	return 0, false
}

const maxVarbinaryWidth int32 = 65535

func isPlanBinaryType(id int32) bool {
	switch id {
	case planBinaryTypeID, planVarbinaryTypeID, planBlobTypeID:
		return true
	default:
		return false
	}
}

func isPlanStringType(id int32) bool {
	switch id {
	case planCharTypeID, planVarcharTypeID, planTextTypeID,
		planBinaryTypeID, planVarbinaryTypeID, planBlobTypeID:
		return true
	default:
		return false
	}
}

func isChangedCharacterSliceResultContract(expr *Expr, functionID int32, name string) bool {
	if !isPlanStringType(expr.Typ.Id) {
		return false
	}
	fn := expr.GetF()
	if fn == nil {
		return false
	}
	var source, length *Expr
	switch {
	case functionID == leftFunctionID || functionID == rightFunctionID || name == "left" || name == "right":
		if len(fn.Args) != 2 {
			return false
		}
		source, length = fn.Args[0], fn.Args[1]
	case functionID == substringFunctionID || name == "substring" || name == "substr" || name == "mid":
		// The two-argument SUBSTRING(source, start) has no fixed output
		// length. Only the explicit length form can be narrower than the
		// already source-derived legacy metadata.
		if len(fn.Args) != 3 {
			return false
		}
		source, length = fn.Args[0], fn.Args[2]
	default:
		return false
	}
	if !isKnownIntegerLiteral(length) || expr.Typ.Width < 0 {
		return false
	}
	if sourceWidth, known := expressionCharacterWidth(source); known {
		return expr.Typ.Width < sourceWidth
	}
	if isPlanBinaryType(expr.Typ.Id) {
		return expr.Typ.Width != maxVarbinaryWidth
	}
	return expr.Typ.Width != maxVarcharWidth
}

func conditionalFirstValue(fn *Function, functionID int32, name string) *Expr {
	if fn == nil || len(fn.Args) == 0 {
		return nil
	}
	switch {
	case functionID == coalesceFunctionID || name == "coalesce":
		return fn.Args[0]
	case functionID == caseFunctionID || functionID == iffFunctionID ||
		name == "case" || name == "if" || name == "iff":
		if len(fn.Args) > 1 {
			return fn.Args[1]
		}
	}
	return nil
}

func expressionSourceType(expr *Expr) Type {
	if expr == nil {
		return Type{}
	}
	fn := expr.GetF()
	if fn != nil && fn.Func != nil && fn.Func.GetObjName() == "cast" &&
		!fn.GetSyntaxExplicitCast() && len(fn.Args) > 0 {
		return expressionSourceType(fn.Args[0])
	}
	return expr.Typ
}

func conditionalValueSources(fn *Function, functionID int32, name string) (values []*Expr, omittedElse bool) {
	if fn == nil {
		return nil, false
	}
	switch {
	case functionID == caseFunctionID || name == "case":
		// CASE arguments are condition/value pairs, followed by an optional
		// ELSE value. An even number of arguments therefore means that the
		// implicit ELSE NULL is part of the result contract.
		for i := 1; i < len(fn.Args); i += 2 {
			values = append(values, fn.Args[i])
		}
		if len(fn.Args)%2 == 1 && len(fn.Args) > 0 {
			values = append(values, fn.Args[len(fn.Args)-1])
		}
		return values, len(fn.Args)%2 == 0
	case functionID == iffFunctionID || name == "if" || name == "iff":
		if len(fn.Args) >= 3 {
			return fn.Args[1:3], false
		}
	}
	return nil, false
}

func conditionalConditionNeedsMetadataFence(fn *Function, functionID int32, name string) bool {
	if fn == nil {
		return false
	}
	switch {
	case functionID == caseFunctionID || name == "case":
		// CASE conditions are lowered to BOOL. Preserve the original source
		// type so a legacy condition cast cannot silently select the old
		// temporal overload.
		for i := 0; i+1 < len(fn.Args); i += 2 {
			if expressionSourceType(fn.Args[i]).Id != planBooleanTypeID {
				return true
			}
		}
	case functionID == iffFunctionID || name == "if" || name == "iff":
		// IF/IFF intentionally keep the legacy numeric/string condition
		// behavior. Only an unresolved ANY condition needs the new fence.
		return len(fn.Args) == 0 || expressionSourceType(fn.Args[0]).Id == planAnyTypeID
	}
	return false
}

func isChangedTemporalConditionalResultContract(expr *Expr, functionID int32, name string) bool {
	if !isPlanTemporalType(expr.Typ.Id) || expr.Typ.Scale <= 0 {
		return false
	}
	fn := expr.GetF()
	switch {
	case functionID == coalesceFunctionID || name == "coalesce":
		// DATETIME used the first branch's FSP historically; TIME used the
		// zero-FSP overload. TIMESTAMP already merged source FSP before this
		// follow-up and therefore needs no new fence.
		if expr.Typ.Id == planTimeTypeID {
			return true
		}
		if expr.Typ.Id != planDatetimeTypeID {
			return false
		}
		first := expressionSourceType(conditionalFirstValue(fn, functionID, name))
		return expr.Typ.Scale > first.Scale
	case functionID == caseFunctionID || functionID == iffFunctionID ||
		name == "case" || name == "if" || name == "iff":
		if conditionalConditionNeedsMetadataFence(fn, functionID, name) {
			return true
		}
		values, omittedElse := conditionalValueSources(fn, functionID, name)
		if omittedElse {
			return true
		}
		for _, value := range values {
			source := expressionSourceType(value)
			if source.Id == planAnyTypeID {
				return true
			}
			if isPlanTemporalType(source.Id) && source.Id != expr.Typ.Id {
				return true
			}
		}
		first := expressionSourceType(conditionalFirstValue(fn, functionID, name))
		return expr.Typ.Scale > first.Scale
	default:
		return false
	}
}

// isExpressionResultMetadataContract identifies only the follow-up metadata
// changes from the result-contract fixes. It deliberately keys off the
// serialized function identity and result type rather than fencing every
// conditional or string expression at v86. Legacy plans with the old
// unbounded slice metadata or zero-FSP conditional metadata remain usable.
func isExpressionResultMetadataContract(expr *Expr) bool {
	if expr == nil {
		return false
	}
	fn := expr.GetF()
	if fn == nil || fn.Func == nil {
		return false
	}
	functionID := int32(fn.Func.Obj >> 32)
	name := strings.ToLower(fn.Func.GetObjName())
	switch functionID {
	case leftFunctionID, rightFunctionID, substringFunctionID:
		return isChangedCharacterSliceResultContract(expr, functionID, name)
	case caseFunctionID, coalesceFunctionID, iffFunctionID:
		return isChangedTemporalConditionalResultContract(expr, functionID, name)
	default:
		switch name {
		case "left", "right", "substring", "substr", "mid":
			return isChangedCharacterSliceResultContract(expr, functionID, name)
		case "case", "coalesce", "if", "iff":
			return isChangedTemporalConditionalResultContract(expr, functionID, name)
		default:
			return false
		}
	}
}

// RequiredRemoteExpressionFeatures reports the independent versioned
// expression features present in owner. Keep the features separate so callers
// can retain precise diagnostics, while sharing one owner walk so adding one
// feature cannot accidentally replace another feature's compatibility gate.
func RequiredRemoteExpressionFeatures(owner any) (features RemoteExpressionFeatures, err error) {
	err = walkExpressionsInOwner(owner, func(expr *Expr) error {
		return VisitExprTree(expr, func(current *Expr) error {
			if !features.DecimalLiteralSemantics {
				if literal := current.GetLit(); literal != nil {
					features.DecimalLiteralSemantics = literal.DecimalLiteralRequiresV82
				}
				if literalVec := current.GetVec(); literalVec != nil {
					features.DecimalLiteralSemantics = literalVec.DecimalLiteralRequiresV82
				}
			}
			fn := current.GetF()
			if fn != nil && fn.Func != nil {
				id, overload := int32(fn.Func.Obj>>32), int32(fn.Func.Obj)
				// DIV overload 0 keeps its function identity, but v97 changes
				// decimal result scale and coefficient interpretation.
				if id == 13 && overload == 0 &&
					(current.Typ.Id == 32 || current.Typ.Id == 33 || current.Typ.Id == 34) {
					features.DecimalDivisionSemantics = true
				}
				// Function IDs live in the function registry, which cannot be
				// imported here because the planner depends on this package.
				if id == 189 { // legacy TO_INTERVAL: ambiguous across pre-v97 and v97 binaries
					features.LegacyIntervalUnits = true
				}
				if id == 583 { // TO_INTERVAL_MICROSECOND
					features.NormalizedIntervalUnits = true
				}
				if (id == 224 || id == 225) && overload >= 8 && overload <= 15 { // DATE_ADD/SUB raw TIME interval
					features.NormalizedIntervalUnits = true
				}
				if (id == 205 || id == 218 || id == 141) && overload == 2 { // DAY/YEAR/MONTH(VARCHAR) raw field contract
					features.TemporalResultContracts = true
				}
				if id == 216 && (overload == 0 || overload == 1) { // one-arg WEEK
					features.WeekSessionDefault = true
				}
				// Released overloads retain their physical vector ABI. The new
				// numeric EXTRACT and string ADDTIME/SUBTIME results use appended
				// identities, so 4.2 catalog expressions remain executable.
				if id == 208 && overload >= 0 && overload <= 9 {
					want := planVarcharTypeID
					if overload == 1 {
						want = planUint32TypeID
					}
					if overload >= 5 {
						want = planInt64TypeID
						features.TemporalResultContracts = true
					}
					features.InvalidTemporalResultContract = features.InvalidTemporalResultContract || current.Typ.Id != want
				}
				if (id == 41 && overload >= 6 && overload <= 11) || (id == 378 && overload >= 6 && overload <= 15) {
					want := planDatetimeTypeID
					preparedTime := false
					if (id == 41 && overload >= 9) || (id == 378 && overload >= 11) {
						want = planVarcharTypeID
						// A direct prepared first marker has a TIME(6) result;
						// its string payload still uses this executor.
						preparedTime = current.Typ.Id == planTimeTypeID
						features.TemporalResultContracts = true
					}
					features.InvalidTemporalResultContract = features.InvalidTemporalResultContract || (!preparedTime && current.Typ.Id != want)
				}
				// Stable TIME integer identities also changed the endpoint/error
				// contract. In 4.2, 838:59:59 + 1 SECOND returned 839:00:00;
				// newly bound execution returns NULL + warning 1441.
				if (id == 224 && overload == 5) || (id == 225 && overload == 6) {
					features.TemporalResultContracts = true
				}

				if (id == 72 || id == 103) && len(fn.Args) == 2 &&
					hasPrivateIntegerPrecisionCast(fn.Args[1]) {
					features.PreparedPrecisionScalar = true
				}
				// CAST is stable function ID 21. Match execution identity, not
				// names or source types; legacy CAST 0..4 remains executable.
				if id == 21 && overload >= 5 && overload <= 8 {
					if err := validateIntegerArgumentCast(current, overload); err != nil {
						return err
					}
					features.IntegerParameterCoercion = true
				}
				// PLUS/MINUS/MULTI are stable function IDs 10/11/12.
				if (id >= 10 && id <= 12 && overload == 2) || (id == 11 && overload == 3) {
					features.IntegerArithmeticDomains = true
				}
			}
			if !features.NumericPrefix && current.Typ.Charset == 255 && fn != nil && fn.Func != nil &&
				strings.EqualFold(fn.Func.GetObjName(), "cast") {
				features.NumericPrefix = true
			}
			if !features.JSONComparisonParam && fn != nil && fn.Func != nil &&
				int32(fn.Func.Obj>>32) == internalJSONComparisonFunctionID {
				features.JSONComparisonParam = true
			}
			if !features.MixedJSONBooleanEquality && isMixedJSONBooleanEquality(fn) {
				features.MixedJSONBooleanEquality = true
			}
			formatNumericArguments, err := isNumericFormatFunction(fn)
			if err != nil {
				return err
			}
			if formatNumericArguments {
				features.FormatNumericArguments = true
			}
			if !features.TypedConversionFunctions && isTypedConversionFunction(fn) {
				features.TypedConversionFunctions = true
			}
			if fn != nil && fn.Func != nil && int32(fn.Func.Obj>>32) == convFunctionID && len(fn.Args) == 3 {
				for _, base := range fn.Args[1:] {
					// Only literal INT64 bases prove the pre-v65 constant-vector
					// contract. Unknown or not-yet-folded expressions fail closed.
					if base == nil || base.Typ.Id != 23 || base.GetLit() == nil {
						features.RowDependentConvBases = true
					}
				}
			}
			if !features.ASCIIInt32Result && isASCIIInt32Result(current) {
				features.ASCIIInt32Result = true
			}
			if !features.StringNumericResultContracts && isStringNumericResultContract(current) {
				features.StringNumericResultContracts = true
			}
			if !features.BoundedConditionalStringDomains && isBoundedConditionalStringDomain(fn) {
				features.BoundedConditionalStringDomains = true
			}
			if !features.TOBase64ResultContracts && isTOBase64ResultContract(current) {
				features.TOBase64ResultContracts = true
			}
			if !features.IPFunctionResultContracts && isIPFunctionResultContract(current) {
				features.IPFunctionResultContracts = true
			}
			if !features.ExpressionResultMetadataContracts && isExpressionResultMetadataContract(current) {
				features.ExpressionResultMetadataContracts = true
			}
			if !features.IPFunctionSemantics && fn != nil && fn.Func != nil {
				features.IPFunctionSemantics = isRemoteIPFunction(int32(fn.Func.Obj >> 32))
			}
			if !features.SpatialDistanceSemantics && fn != nil && fn.Func != nil {
				functionID := int32(fn.Func.Obj >> 32)
				overloadID := int32(fn.Func.Obj)
				switch functionID {
				case remoteFrechetDistanceFunctionID, remoteHausdorffDistanceFunctionID:
					// Overloads 0/1 are the historical planar identities. The
					// unit overloads 2/3 and the corrected two-argument
					// geodetic identities 4/5 are v90 contracts.
					features.SpatialDistanceSemantics = overloadID >= 2 && overloadID <= 5
				case remoteSpatialDistanceFunctionID:
					// ST_DISTANCE overloads 4/5 are the new length-unit forms.
					// Legacy two-argument and explicit-SRID forms retain their
					// historical wire contract.
					features.SpatialDistanceSemantics = overloadID == 4 || overloadID == 5
				}
			}
			return nil
		})
	})
	return
}

// isASCIIInt32Result identifies the new physical result contract of ASCII.
// Older serialized plans use the same overload IDs with UINT8 results, so the
// result type must participate in the feature check; gating every ASCII call
// would unnecessarily block compatible legacy plans during a rolling upgrade.
func isASCIIInt32Result(expr *Expr) bool {
	if expr == nil || expr.Typ.Id != asciiInt32ResultTypeID {
		return false
	}
	fn := expr.GetF()
	if fn == nil || fn.Func == nil {
		return false
	}
	return int32(fn.Func.Obj>>32) == asciiFunctionID ||
		strings.EqualFold(fn.Func.GetObjName(), "ascii")
}

// isStringNumericResultContract identifies the new physical result contracts
// of the affected string numeric functions. Legacy serialized plans use the
// same overload IDs with their historical result wrappers, so the result type
// participates in the feature check just as it does for ASCII.
func isStringNumericResultContract(expr *Expr) bool {
	if expr == nil {
		return false
	}
	fn := expr.GetF()
	if fn == nil || fn.Func == nil {
		return false
	}
	functionID := int32(fn.Func.Obj >> 32)
	name := strings.ToLower(fn.Func.GetObjName())
	switch functionID {
	case findInSetFunctionID, strCmpFunctionID:
		return expr.Typ.Id == stringNumericInt32ResultTypeID
	case lengthUTF8FunctionID, uncompressedLengthFunctionID:
		return expr.Typ.Id == stringNumericInt64ResultTypeID
	case crc32FunctionID:
		return expr.Typ.Id == stringNumericUint64ResultTypeID
	default:
		switch name {
		case "find_in_set", "findinset", "strcmp":
			return expr.Typ.Id == stringNumericInt32ResultTypeID
		case "char_length", "character_length", "length_utf8", "uncompressed_length":
			return expr.Typ.Id == stringNumericInt64ResultTypeID
		case "crc32":
			return expr.Typ.Id == stringNumericUint64ResultTypeID
		default:
			return false
		}
	}
}

// RequiresMORPCVersion64TypedConversion reports whether an owner contains a
// BIN/CONV expression whose serialized overload or physical argument contract
// is not executable with the pre-v64 function registry.
func RequiresMORPCVersion64TypedConversion(owner any) (bool, error) {
	features, err := RequiredRemoteExpressionFeatures(owner)
	return features.TypedConversionFunctions, err
}

// RequiresMORPCVersion65ASCIIResult reports whether an owner contains the
// signed INT physical result contract introduced for ASCII.
func RequiresMORPCVersion65ASCIIResult(owner any) (bool, error) {
	features, err := RequiredRemoteExpressionFeatures(owner)
	return features.ASCIIInt32Result, err
}

// isTypedConversionFunction identifies only the BIN/CONV forms changed by the
// typed-dispatch fix. Plain string BIN/CONV and integer BIN retain their old
// wire/execution contract and remain usable during a rolling upgrade.
func isTypedConversionFunction(function *Function) bool {
	if function == nil || function.Func == nil {
		return false
	}

	functionID := int32(function.Func.Obj >> 32)
	name := strings.ToLower(function.Func.GetObjName())
	firstType := int32(0)
	if len(function.Args) > 0 && function.Args[0] != nil {
		firstType = function.Args[0].Typ.Id
	}
	overloadID := int32(function.Func.Obj)

	switch {
	case functionID == convFunctionID || name == "conv":
		// CONV overloads 3..12 are the typed integer/float forms. The dynamic
		// overload is 13. BOOL/DECIMAL/temporal values use the historical
		// overload 0 but carry a fixed-width vector, so inspect the argument
		// type as well.
		return overloadID >= 3 || !isPlanMySQLStringType(firstType)
	case functionID == binFunctionID || name == "bin":
		// BIN overloads 8/9 are FLOAT32/FLOAT64 and now use MySQL's numeric
		// prefix representation. Overload 11 is the new dynamic fixed-width
		// path; overload 10 remains the string path.
		return overloadID == 8 || overloadID == 9 || overloadID >= 11 ||
			firstType == 30 || firstType == 31 // FLOAT32/FLOAT64
	default:
		return false
	}
}

// FORMAT reuses its historical VARCHAR overload IDs for the new typed numeric
// execution path. A pre-v59 receiver still interprets those vectors as
// Varlena and can panic while decoding the first argument, so this physical
// argument contract must be fenced at the remote pipeline boundary.
func isNumericFormatFunction(function *Function) (bool, error) {
	if function == nil || function.Func == nil || len(function.Args) < 2 {
		return false, nil
	}
	const formatFunctionID int32 = 262
	functionID := int32(function.Func.Obj >> 32)
	if functionID != formatFunctionID && !strings.EqualFold(function.Func.GetObjName(), "format") {
		return false, nil
	}
	if function.Args[0] == nil {
		return false, moerr.NewInvalidInputNoCtx("FORMAT is missing its first argument")
	}
	return isPlanNumericType(function.Args[0].Typ.Id), nil
}

// isPlanNumericType mirrors container/types.Type.IsNumeric without importing
// that package (container/types itself depends on pb/plan). The IDs are part
// of the plan wire contract and include BIT, integer, floating-point and
// decimal families.
func isPlanNumericType(id int32) bool {
	switch id {
	case 11, // BIT
		20, 21, 22, 23, // signed integers
		25, 26, 27, 28, // unsigned integers
		30, 31, // floating point
		32, 33, 34: // decimals
		return true
	default:
		return false
	}
}

func isMixedJSONBooleanEquality(function *Function) bool {
	if function == nil || function.Func == nil || len(function.Args) != 2 {
		return false
	}
	functionID := int32(function.Func.Obj >> 32)
	switch functionID {
	case equalFunctionID, notEqualFunctionID, nullSafeEqualFunctionID:
	default:
		return false
	}
	leftType := function.Args[0].Typ.Id
	rightType := function.Args[1].Typ.Id
	return leftType == planJSONTypeID && rightType == planBooleanTypeID ||
		leftType == planBooleanTypeID && rightType == planJSONTypeID
}

func (m *Expr) possibleRuntimeStringDomains() (uint8, bool, error) {
	if m == nil {
		return 0, false, nil
	}
	staticDomains := possibleStringDomainForStaticType(m.Typ)
	if lit := m.GetLit(); lit != nil {
		if err := m.validateStringLiteralForm(lit); err != nil {
			return 0, false, err
		}
		domains := staticDomains
		ownRequired := false
		switch lit.LiteralForm {
		case StringLiteralForm_STRING_LITERAL_TEXT:
			domains = possibleStringDomainText
			ownRequired = staticDomains == possibleStringDomainBinary
		case StringLiteralForm_STRING_LITERAL_BINARY_INTRODUCER,
			StringLiteralForm_STRING_LITERAL_HEX,
			StringLiteralForm_STRING_LITERAL_BIT:
			domains = possibleStringDomainBinary
			ownRequired = lit.LiteralForm == StringLiteralForm_STRING_LITERAL_BINARY_INTRODUCER &&
				staticDomains == possibleStringDomainText
		}
		_, childRequired, err := lit.Src.possibleRuntimeStringDomains()
		return domains, childRequired || ownRequired, err
	}

	fn := m.GetF()
	if fn == nil {
		required := false
		visit := func(expr *Expr) error {
			_, childRequired, err := expr.possibleRuntimeStringDomains()
			required = required || childRequired
			return err
		}
		if list := m.GetList(); list != nil {
			for _, item := range list.List {
				if err := visit(item); err != nil {
					return 0, false, err
				}
			}
		}
		if subquery := m.GetSub(); subquery != nil {
			if err := visit(subquery.Child); err != nil {
				return 0, false, err
			}
		}
		if window := m.GetW(); window != nil {
			if err := visit(window.WindowFunc); err != nil {
				return 0, false, err
			}
			for _, item := range window.PartitionBy {
				if err := visit(item); err != nil {
					return 0, false, err
				}
			}
			for _, order := range window.OrderBy {
				if order != nil {
					if err := visit(order.Expr); err != nil {
						return 0, false, err
					}
				}
			}
			if window.Frame != nil {
				if window.Frame.Start != nil {
					if err := visit(window.Frame.Start.Val); err != nil {
						return 0, false, err
					}
				}
				if window.Frame.End != nil {
					if err := visit(window.Frame.End.Val); err != nil {
						return 0, false, err
					}
				}
			}
		}
		return staticDomains, required, nil
	}
	required := false
	argDomains := make([]uint8, len(fn.Args))
	for i, arg := range fn.Args {
		domains, argRequired, err := arg.possibleRuntimeStringDomains()
		if err != nil {
			return 0, false, err
		}
		argDomains[i] = domains
		required = required || argRequired
	}
	name := ""
	functionID := int32(0)
	if fn.Func != nil {
		name = strings.ToLower(fn.Func.ObjName)
		functionID = int32(fn.Func.Obj >> 32)
	}
	if (name == "cast" || functionID == 21) &&
		len(argDomains) != 0 && fn.Func != nil && int32(fn.Func.Obj) == 0 {
		// The low 32 bits encode the overload. Overload zero is the binder's
		// implicit cast and is transparent to flow-control selected values.
		return argDomains[0], required, nil
	}

	selectedDomains := uint8(0)
	switch {
	case name == "if" || name == "iff" || functionID == 113:
		for i := 1; i < len(argDomains); i++ {
			selectedDomains |= argDomains[i]
		}
	case name == "case" || functionID == 71:
		for i := 1; i < len(argDomains); i += 2 {
			selectedDomains |= argDomains[i]
		}
		if len(argDomains)%2 == 1 {
			selectedDomains |= argDomains[len(argDomains)-1]
		}
	case name == "coalesce" || functionID == 74:
		for _, domains := range argDomains {
			selectedDomains |= domains
		}
	default:
		return staticDomains, required, nil
	}
	if selectedDomains != 0 && selectedDomains&^staticDomains != 0 {
		required = true
	}
	return selectedDomains, required, nil
}

func (m *Expr) walkStringLiterals(visitor func(*Expr, *Literal) error) error {
	if m == nil {
		return nil
	}
	if lit := m.GetLit(); lit != nil {
		if err := visitor(m, lit); err != nil {
			return err
		}
		return lit.Src.walkStringLiterals(visitor)
	}
	if fn := m.GetF(); fn != nil {
		for _, arg := range fn.Args {
			if err := arg.walkStringLiterals(visitor); err != nil {
				return err
			}
		}
	}
	if list := m.GetList(); list != nil {
		for _, item := range list.List {
			if err := item.walkStringLiterals(visitor); err != nil {
				return err
			}
		}
	}
	if subquery := m.GetSub(); subquery != nil {
		if err := subquery.Child.walkStringLiterals(visitor); err != nil {
			return err
		}
	}
	if window := m.GetW(); window != nil {
		if err := window.WindowFunc.walkStringLiterals(visitor); err != nil {
			return err
		}
		for _, item := range window.PartitionBy {
			if err := item.walkStringLiterals(visitor); err != nil {
				return err
			}
		}
		for _, order := range window.OrderBy {
			if order != nil {
				if err := order.Expr.walkStringLiterals(visitor); err != nil {
					return err
				}
			}
		}
		if window.Frame != nil {
			if window.Frame.Start != nil {
				if err := window.Frame.Start.Val.walkStringLiterals(visitor); err != nil {
					return err
				}
			}
			if window.Frame.End != nil {
				return window.Frame.End.Val.walkStringLiterals(visitor)
			}
		}
	}
	return nil
}

func (p *Plan) ValidateStringLiteralForms() error {
	return validateStringLiteralFormsInOwner(p)
}

func ValidateStringLiteralFormsInOwner(owner any) error {
	return validateStringLiteralFormsInOwner(owner)
}

// VisitExprTree visits expr and every nested expression in deterministic order.
func VisitExprTree(expr *Expr, visitor func(*Expr) error) error {
	if expr == nil {
		return nil
	}
	if err := visitor(expr); err != nil {
		return err
	}
	if lit := expr.GetLit(); lit != nil {
		if err := VisitExprTree(lit.Src, visitor); err != nil {
			return err
		}
	}
	if fn := expr.GetF(); fn != nil {
		for _, arg := range fn.Args {
			if err := VisitExprTree(arg, visitor); err != nil {
				return err
			}
		}
	}
	if list := expr.GetList(); list != nil {
		for _, item := range list.List {
			if err := VisitExprTree(item, visitor); err != nil {
				return err
			}
		}
	}
	if sub := expr.GetSub(); sub != nil {
		if err := VisitExprTree(sub.Child, visitor); err != nil {
			return err
		}
	}
	if window := expr.GetW(); window != nil {
		if err := VisitExprTree(window.WindowFunc, visitor); err != nil {
			return err
		}
		for _, item := range window.PartitionBy {
			if err := VisitExprTree(item, visitor); err != nil {
				return err
			}
		}
		for _, order := range window.OrderBy {
			if order != nil {
				if err := VisitExprTree(order.Expr, visitor); err != nil {
					return err
				}
			}
		}
		if window.Frame != nil {
			if window.Frame.Start != nil {
				if err := VisitExprTree(window.Frame.Start.Val, visitor); err != nil {
					return err
				}
			}
			if window.Frame.End != nil {
				if err := VisitExprTree(window.Frame.End.Val, visitor); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// VisitExpressionsInOwner visits each expression root contained in an owner.
func VisitExpressionsInOwner(owner any, visitor func(*Expr) error) error {
	return walkExpressionsInOwner(owner, visitor)
}

// validateStringLiteralFormsInOwner validates every expression nested in a
// decoded plan without coupling this boundary check to every plan node shape.
func validateStringLiteralFormsInOwner(owner any) error {
	return walkExpressionsInOwner(owner, func(expr *Expr) error {
		return expr.ValidateStringLiteralForms()
	})
}

func walkExpressionsInOwner(owner any, visitor func(*Expr) error) error {
	seen := make(map[uintptr]struct{})
	var walk func(reflect.Value) error
	walk = func(value reflect.Value) error {
		if !value.IsValid() {
			return nil
		}
		if value.Kind() == reflect.Interface {
			if value.IsNil() {
				return nil
			}
			return walk(value.Elem())
		}
		if value.Kind() == reflect.Pointer {
			if value.IsNil() {
				return nil
			}
			if expr, ok := value.Interface().(*Expr); ok {
				return visitor(expr)
			}
			pointer := value.Pointer()
			if _, ok := seen[pointer]; ok {
				return nil
			}
			seen[pointer] = struct{}{}
			return walk(value.Elem())
		}
		switch value.Kind() {
		case reflect.Struct:
			for field := 0; field < value.NumField(); field++ {
				if value.Type().Field(field).PkgPath == "" {
					if err := walk(value.Field(field)); err != nil {
						return err
					}
				}
			}
		case reflect.Slice, reflect.Array:
			if value.Type().Elem().Kind() == reflect.Uint8 {
				return nil
			}
			for item := 0; item < value.Len(); item++ {
				if err := walk(value.Index(item)); err != nil {
					return err
				}
			}
		case reflect.Map:
			iterator := value.MapRange()
			for iterator.Next() {
				if err := walk(iterator.Value()); err != nil {
					return err
				}
			}
		}
		return nil
	}
	return walk(reflect.ValueOf(owner))
}
