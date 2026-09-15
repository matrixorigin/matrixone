// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package function

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strings"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/udf"
	pythonudf "github.com/matrixorigin/matrixone/pkg/udf/python"
)

type Udf struct {
	Body         string `json:"body"`
	Language     string `json:"language"`
	RetType      string `json:"rettype"`
	Args         []*Arg `json:"args"`
	Db           string `json:"db"`
	ModifiedTime string `json:"modified_time"`
	// FunctionID and Revision are resolved from the shared catalog. They are
	// part of the executable identity; the planner must never re-resolve a
	// Python routine by name or latest revision at execution time.
	FunctionID       int64  `json:"function_id,omitempty"`
	AccountID        uint64 `json:"account_id,omitempty"`
	DatabaseID       uint64 `json:"database_id,omitempty"`
	Revision         uint64 `json:"revision,omitempty"`
	NamespaceVersion uint64 `json:"namespace_version,omitempty"`
	// SQL revisions use the same immutable identity envelope as Python. These
	// fields are empty for legacy SQL rows, which intentionally remain on the
	// historical text-expansion path until they are explicitly replaced.
	DefinitionFingerprint           string       `json:"definition_fingerprint,omitempty"`
	SemanticDefinitionSchemaVersion int          `json:"semantic_definition_schema_version,omitempty"`
	Volatility                      string       `json:"volatility,omitempty"`
	NullPolicy                      string       `json:"null_policy,omitempty"`
	SQLMode                         *string      `json:"-"`
	ArgsType                        []types.Type `json:"-"`
	// PythonArgTypes and PythonReturnType preserve the exact SQL declaration
	// for Python's Arrow contract. The legacy catalog's args/rettype columns
	// intentionally keep only logical type names, which is insufficient for
	// width and scale validation.
	PythonArgTypes   []PythonTypeDescriptor `json:"-"`
	PythonReturnType *PythonTypeDescriptor  `json:"-"`
}

type UdfWithContext struct {
	*Udf    `json:"udf"`
	Context map[string]string `json:"context"`
	// PlanContractVersion is retained only for the guarded bridge while the
	// typed RoutineCall plan node is rolled out. A missing or unknown value is
	// rejected before the handler is reached.
	PlanContractVersion int `json:"plan_contract_version"`
}

// PythonRoutineBody is the single supported Python routine definition.  The
// catalog must persist mode and NULL policy with the source so execution does
// not infer either from a generic SQL function class bit.
type PythonRoutineBody struct {
	DefinitionSchemaVersion int                    `json:"definition_schema_version"`
	Handler                 string                 `json:"handler"`
	Source                  string                 `json:"source,omitempty"`
	Mode                    string                 `json:"mode"`
	NullPolicy              string                 `json:"null_policy"`
	ABIContract             string                 `json:"abi_contract"`
	AdapterVersion          string                 `json:"adapter_version"`
	ArtifactDigest          string                 `json:"artifact_digest"`
	EnvironmentDigest       string                 `json:"environment_digest"`
	SDKVersion              string                 `json:"sdk_version"`
	ArgTypes                []PythonTypeDescriptor `json:"arg_types,omitempty"`
	ReturnType              *PythonTypeDescriptor  `json:"return_type,omitempty"`
}

// PythonSignatureMetadata is the canonical identity metadata for a Python
// routine. The input descriptor is the overload key; the return descriptor is
// persisted alongside it because it is immutable for the lifetime of the
// Function identity even though it does not participate in overload choice.
// Both JSON values are canonical encodings of the exact Arrow descriptors.
func PythonSignatureMetadata(
	args []PythonTypeDescriptor,
	returnType *PythonTypeDescriptor,
) (canonicalInput, canonicalReturn, fingerprint string, err error) {
	if returnType == nil {
		return "", "", "", fmt.Errorf("python routine is missing return descriptor")
	}
	if args == nil {
		args = []PythonTypeDescriptor{}
	}
	for index, descriptor := range args {
		if err := validatePythonTypeDescriptor(descriptor); err != nil {
			return "", "", "", fmt.Errorf("python routine argument descriptor %d: %w", index, err)
		}
	}
	if err := validatePythonTypeDescriptor(*returnType); err != nil {
		return "", "", "", fmt.Errorf("python routine return descriptor: %w", err)
	}
	inputBytes, err := json.Marshal(args)
	if err != nil {
		return "", "", "", fmt.Errorf("marshal Python input descriptor: %w", err)
	}
	returnBytes, err := json.Marshal(returnType)
	if err != nil {
		return "", "", "", fmt.Errorf("marshal Python return descriptor: %w", err)
	}
	if len(inputBytes) > types.MaxStringSize || len(returnBytes) > types.MaxStringSize {
		return "", "", "", fmt.Errorf("python signature descriptor exceeds the %d-byte catalog limit", types.MaxStringSize)
	}
	keyBytes, err := json.Marshal(struct {
		SchemaVersion int                    `json:"schema_version"`
		Input         []PythonTypeDescriptor `json:"input_descriptor"`
		Return        PythonTypeDescriptor   `json:"return_descriptor"`
	}{
		SchemaVersion: udf.PythonSignatureKeySchemaVersion,
		Input:         args,
		Return:        *returnType,
	})
	if err != nil {
		return "", "", "", fmt.Errorf("marshal Python signature key: %w", err)
	}
	digest := sha256.New()
	digest.Write([]byte("matrixone-python-udf-signature\x00"))
	digest.Write(keyBytes)
	return string(inputBytes), string(returnBytes), hex.EncodeToString(digest.Sum(nil)), nil
}

// DecodePythonRoutineBody decodes the one catalog representation accepted by
// the Python execution path.  Catalog data is an execution authority, so an
// unknown field or a second JSON value must be rejected instead of being
// silently ignored by encoding/json.
func DecodePythonRoutineBody(raw string) (PythonRoutineBody, error) {
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	var body PythonRoutineBody
	if err := decoder.Decode(&body); err != nil {
		return PythonRoutineBody{}, err
	}
	var extra json.RawMessage
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return PythonRoutineBody{}, fmt.Errorf("python routine body contains multiple JSON values")
		}
		return PythonRoutineBody{}, err
	}
	if err := body.Validate(); err != nil {
		return PythonRoutineBody{}, err
	}
	return body, nil
}

// PythonRoutineFingerprint returns the digest of the canonical current
// definition. The digest is persisted with an immutable revision and is
// checked before dispatch; source text alone is not an executable identity.
func PythonRoutineFingerprint(raw string) (string, error) {
	body, err := DecodePythonRoutineBody(raw)
	if err != nil {
		return "", err
	}
	return PythonRoutineBodyFingerprint(body)
}

func PythonRoutineBodyFingerprint(body PythonRoutineBody) (string, error) {
	if err := body.Validate(); err != nil {
		return "", err
	}
	return pythonudf.DefinitionFingerprint(
		body.DefinitionSchemaVersion,
		body.Handler, body.Mode, body.NullPolicy,
		body.ABIContract, body.AdapterVersion,
		body.ArtifactDigest, body.EnvironmentDigest, body.SDKVersion,
		body.ArgTypes, *body.ReturnType,
	)
}

// SQLRoutineFingerprint is the integrity key for the current shared SQL
// revision row.  The full typed SQL semantic IR is a later catalog field;
// until that field is published, the revision's canonical stored argument
// signature, return type, and SQL body form the bounded semantic definition
// accepted by the SQL binder.  Keeping this encoding here makes the writer,
// resolver, and plan-cache validator use one implementation instead of
// independently hashing source text.
func SQLRoutineFingerprint(body, argTypes, returnType string) (string, error) {
	if body == "" || returnType == "" {
		return "", fmt.Errorf("SQL routine definition is incomplete")
	}
	encoded, err := json.Marshal(struct {
		SchemaVersion int    `json:"schema_version"`
		Body          string `json:"body"`
		ArgTypes      string `json:"arg_types"`
		ReturnType    string `json:"return_type"`
	}{
		SchemaVersion: udf.SQLDefinitionSchemaVersion,
		Body:          body,
		ArgTypes:      argTypes,
		ReturnType:    returnType,
	})
	if err != nil {
		return "", fmt.Errorf("marshal SQL routine definition: %w", err)
	}
	digest := sha256.New()
	digest.Write([]byte("matrixone-sql-udf-definition\x00"))
	digest.Write(encoded)
	return hex.EncodeToString(digest.Sum(nil)), nil
}

// DecodeUdfWithContext is retained only as a diagnostic boundary for old
// persisted/demo plans. JSON is not an executable representation of a new
// Python routine, so callers must rebind the statement from the current typed
// catalog contract instead of decoding and dispatching this payload.
func DecodeUdfWithContext(raw []byte) (UdfWithContext, error) {
	_ = raw
	return UdfWithContext{}, fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: Python JSON plan is not executable; rebind the statement from the current typed catalog contract")
}

// Validate checks fields that are independent of the catalog argument list.
// The argument count is checked by Udf.ValidatePythonTypeContract after the
// legacy catalog args column has been decoded.
func (body PythonRoutineBody) Validate() error {
	if body.DefinitionSchemaVersion != udf.PythonDefinitionSchemaVersion {
		return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: python definition schema %d is not supported", body.DefinitionSchemaVersion)
	}
	if body.Handler == "" || body.Source == "" {
		return fmt.Errorf("python routine handler and source are required")
	}
	if !utf8.ValidString(body.Source) {
		return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: Python source is not valid UTF-8")
	}
	if int64(len([]byte(body.Source))) > pythonudf.DefaultMaxArtifactBytes {
		return fmt.Errorf("RESOURCE_EXHAUSTED: Python source exceeds %d bytes", pythonudf.DefaultMaxArtifactBytes)
	}
	if strings.Contains(body.Handler, ":") {
		return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: Python external handler import requires an immutable artifact catalog")
	}
	if body.Mode != "SCALAR" && body.Mode != "VECTOR" {
		return fmt.Errorf("python routine has unsupported mode %q", body.Mode)
	}
	if body.NullPolicy != udf.NullCallHandler && body.NullPolicy != udf.NullReturnNull {
		return fmt.Errorf("python routine has unsupported NULL policy %q", body.NullPolicy)
	}
	if body.ABIContract != udf.PythonABIContract || body.AdapterVersion != udf.PythonAdapterVersion {
		return fmt.Errorf("python routine has unsupported ABI contract %q/%q", body.ABIContract, body.AdapterVersion)
	}
	if body.SDKVersion != udf.PythonSDKVersion {
		return fmt.Errorf("python routine has unsupported SDK %q", body.SDKVersion)
	}
	if !udf.IsSHA256Digest(body.ArtifactDigest) || body.EnvironmentDigest == "" {
		return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: Python definition has no immutable artifact/environment digest; use DROP and CREATE")
	}
	if body.ArtifactDigest != udf.PythonInlineArtifactDigest(body.Handler, body.Source) {
		return fmt.Errorf("python routine artifact digest does not match its source")
	}
	environmentDigest, err := udf.PythonEnvironmentDigest()
	if err != nil {
		return fmt.Errorf("python routine environment digest is unavailable: %w", err)
	}
	if body.EnvironmentDigest != environmentDigest {
		return fmt.Errorf("python routine environment digest does not match the current contract")
	}
	if body.ReturnType == nil {
		return fmt.Errorf("python routine is missing return descriptor")
	}
	for i, descriptor := range body.ArgTypes {
		if err := validatePythonTypeDescriptor(descriptor); err != nil {
			return fmt.Errorf("python routine argument descriptor %d: %w", i, err)
		}
	}
	if err := validatePythonTypeDescriptor(*body.ReturnType); err != nil {
		return fmt.Errorf("python routine return descriptor: %w", err)
	}
	return nil
}

// PythonTypeDescriptor is the single Arrow ABI descriptor used by both plan
// resolution and the Python adapter. The UDF package keeps it as an alias so
// the catalog body and the Arrow metadata cannot drift independently.
type PythonTypeDescriptor = pythonudf.TypeDescriptor

func NewPythonTypeDescriptor(typ types.Type) (PythonTypeDescriptor, error) {
	return pythonudf.NewTypeDescriptor(typ)
}

type Arg struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// GetRoutineCall materializes the immutable typed call metadata used by the
// physical external evaluator. Statement identity and session values are
// deliberately absent: a prepared plan may outlive the statement that bound
// it, so the physical evaluator must construct those values from the current
// process at execution time.
func (u *Udf) GetRoutineCall() (*plan.RoutineCall, error) {
	if u == nil {
		return nil, fmt.Errorf("routine call requires a non-nil routine")
	}
	if u.Language == udf.LanguageSQL {
		return u.getSQLRoutineCall()
	}
	if u.Language != udf.LanguagePython {
		return nil, fmt.Errorf("routine call has unsupported language %q", u.Language)
	}
	if u.FunctionID <= 0 || u.DatabaseID == 0 || u.Revision == 0 || u.NamespaceVersion == 0 {
		return nil, fmt.Errorf("python routine has no stable catalog identity or revision")
	}
	body, err := DecodePythonRoutineBody(u.Body)
	if err != nil {
		return nil, err
	}
	fingerprint, err := PythonRoutineFingerprint(u.Body)
	if err != nil {
		return nil, err
	}
	// The decoded immutable body is the source of truth for the executable
	// Arrow signature. Do not rebuild it from the mutable legacy Udf fields;
	// doing so could produce a plan whose fingerprint describes one signature
	// while its argument types describe another damaged catalog row.
	argTypes := make([]types.Type, len(body.ArgTypes))
	for i, descriptor := range body.ArgTypes {
		argTypes[i] = descriptor.Type()
	}
	arguments := make([]*plan.Type, len(argTypes))
	for i, typ := range argTypes {
		arguments[i] = type2PlanType(typ)
	}
	returnType := type2PlanType(body.ReturnType.Type())
	return &plan.RoutineCall{
		ContractVersion: udf.PythonPlanContractVersion,
		FunctionRef: &plan.FunctionRef{
			FunctionId:       uint64(u.FunctionID),
			Revision:         u.Revision,
			NamespaceVersion: u.NamespaceVersion,
			AccountId:        u.AccountID,
			DatabaseId:       u.DatabaseID,
		},
		Language:      udf.LanguagePython,
		ArgumentTypes: arguments,
		ReturnType:    *returnType,
		Volatility:    "VOLATILE",
		NullPolicy:    body.NullPolicy,
		MayError:      true,
		SecurityMode:  "INVOKER",
		Leakproof:     false,
		Implementation: &plan.RoutineCall_Python{Python: &plan.PythonRoutineImplementation{
			Handler:                 body.Handler,
			AbiContract:             body.ABIContract,
			AdapterVersion:          body.AdapterVersion,
			ArtifactDigest:          body.ArtifactDigest,
			EnvironmentDigest:       body.EnvironmentDigest,
			SdkVersion:              body.SDKVersion,
			DefinitionSchemaVersion: int32(body.DefinitionSchemaVersion),
			Mode:                    body.Mode,
			NullPolicy:              body.NullPolicy,
			DefinitionFingerprint:   fingerprint,
		}},
	}, nil
}

// getSQLRoutineCall builds the common catalog envelope for a current SQL
// revision. SQL's existing expression lowering still expands the body at
// bind time, but the exact revision and integrity key are retained in the
// plan dependency so replace/drop cannot silently reuse the old definition.
func (u *Udf) getSQLRoutineCall() (*plan.RoutineCall, error) {
	if u.FunctionID <= 0 || u.DatabaseID == 0 || u.Revision == 0 || u.NamespaceVersion == 0 {
		return nil, fmt.Errorf("SQL routine has no stable catalog identity or revision")
	}
	if u.SemanticDefinitionSchemaVersion != udf.SQLDefinitionSchemaVersion || !udf.IsSHA256Digest(u.DefinitionFingerprint) {
		return nil, fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: SQL routine has no supported semantic definition")
	}
	argTypes := u.GetArgsType()
	arguments := make([]*plan.Type, len(argTypes))
	for index, typ := range argTypes {
		arguments[index] = type2PlanType(typ)
	}
	returnType := type2PlanType(u.GetRetType())
	volatility := u.Volatility
	if volatility == "" {
		volatility = "VOLATILE"
	}
	nullPolicy := u.NullPolicy
	if nullPolicy == "" {
		nullPolicy = udf.NullCallHandler
	}
	return &plan.RoutineCall{
		ContractVersion: udf.RoutinePlanContractVersion,
		FunctionRef: &plan.FunctionRef{
			FunctionId:       uint64(u.FunctionID),
			Revision:         u.Revision,
			NamespaceVersion: u.NamespaceVersion,
			AccountId:        u.AccountID,
			DatabaseId:       u.DatabaseID,
		},
		Language:      udf.LanguageSQL,
		ArgumentTypes: arguments,
		ReturnType:    *returnType,
		Volatility:    volatility,
		NullPolicy:    nullPolicy,
		MayError:      true,
		SecurityMode:  "DEFINER",
		Leakproof:     false,
		Implementation: &plan.RoutineCall_Sql{Sql: &plan.SqlRoutineImplementation{
			DefinitionFingerprint:           []byte(u.DefinitionFingerprint),
			SemanticDefinitionSchemaVersion: int32(u.SemanticDefinitionSchemaVersion),
		}},
	}, nil
}

func (u *Udf) GetArgsPlanType() []*plan.Type {
	args := u.GetArgsType()
	result := make([]*plan.Type, len(args))
	for i, typ := range args {
		result[i] = type2PlanType(typ)
	}
	return result
}
func (u *Udf) GetRetPlanType() *plan.Type { return type2PlanType(u.GetRetType()) }
func (u *Udf) GetArgsType() []types.Type {
	if u.Language == udf.LanguagePython {
		// Resolver-created routines always set PythonReturnType when the
		// canonical descriptor has been loaded.  Keeping the fallback makes
		// the generic Udf value usable by planner tests and by the SQL-side
		// preflight checks without allowing a real Python call to skip body
		// validation: GetRoutineCall decodes and validates Body again.
		if u.PythonReturnType == nil && u.PythonArgTypes == nil {
			return u.ArgsType
		}
		args := make([]types.Type, len(u.PythonArgTypes))
		for i, descriptor := range u.PythonArgTypes {
			args[i] = descriptor.Type()
		}
		return args
	}
	return u.ArgsType
}
func (u *Udf) GetRetType() types.Type {
	if u.Language == udf.LanguagePython {
		if u.PythonReturnType == nil {
			return types.Types[u.RetType].ToType()
		}
		return u.PythonReturnType.Type()
	}
	return types.Types[u.RetType].ToType()
}

// LoadPythonTypeContract loads the exact type metadata persisted in the Python
// routine body. Python execution has one ABI, so an incomplete body is
// rejected instead of being reinterpreted through the legacy logical-type
// columns.
func (u *Udf) LoadPythonTypeContract() error {
	if u == nil || u.Language == udf.LanguageSQL {
		return nil
	}
	if u.Language != udf.LanguagePython {
		return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: unsupported routine language %q", u.Language)
	}
	u.PythonArgTypes = nil
	u.PythonReturnType = nil
	body, err := DecodePythonRoutineBody(u.Body)
	if err != nil {
		return err
	}
	u.PythonArgTypes = append([]PythonTypeDescriptor(nil), body.ArgTypes...)
	returnType := *body.ReturnType
	u.PythonReturnType = &returnType
	return nil
}

// ValidatePythonTypeContract checks the descriptor count after catalog
// argument names have been decoded. LoadPythonTypeContract deliberately runs
// before that decode during lookup, so this second check closes the only point
// where an argument list and its exact ABI could otherwise diverge.
func (u *Udf) ValidatePythonTypeContract() error {
	if u == nil || u.Language == udf.LanguageSQL {
		return nil
	}
	if u.Language != udf.LanguagePython {
		return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: unsupported routine language %q", u.Language)
	}
	if u.PythonReturnType == nil {
		return fmt.Errorf("python routine is missing return descriptor")
	}
	if len(u.PythonArgTypes) != len(u.Args) {
		return fmt.Errorf("python routine has %d descriptors for %d arguments", len(u.PythonArgTypes), len(u.Args))
	}
	for i, descriptor := range u.PythonArgTypes {
		if err := validatePythonTypeDescriptor(descriptor); err != nil {
			return fmt.Errorf("python routine argument descriptor %d: %w", i, err)
		}
	}
	return validatePythonTypeDescriptor(*u.PythonReturnType)
}

// ValidatePythonCatalogSignature verifies the logical type columns that are
// retained by mo_user_defined_function/mo_function_revisions against the
// exact Arrow descriptors in the Python implementation.  The legacy columns
// intentionally omit width and scale, so they cannot replace the descriptor;
// they still must agree on the logical OID or a damaged catalog row could be
// selected by overload resolution and dispatched with a different ABI.
func (u *Udf) ValidatePythonCatalogSignature() error {
	if u == nil || u.Language == udf.LanguageSQL {
		return nil
	}
	if u.Language != udf.LanguagePython {
		return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: unsupported routine language %q", u.Language)
	}
	if len(u.Args) != len(u.PythonArgTypes) || u.PythonReturnType == nil {
		return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: Python catalog signature is incomplete")
	}
	for index, argument := range u.Args {
		if argument == nil || !pythonCatalogTypeNameMatches(argument.Type, u.PythonArgTypes[index].Type().Oid) {
			name := "<nil>"
			if argument != nil {
				name = argument.Type
			}
			return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: Python catalog argument %d type %q does not match its descriptor", index+1, name)
		}
	}
	if !pythonCatalogTypeNameMatches(u.RetType, u.PythonReturnType.Type().Oid) {
		return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: Python catalog return type %q does not match its descriptor", u.RetType)
	}
	return nil
}

func pythonCatalogTypeNameMatches(name string, oid types.T) bool {
	name = strings.ToLower(strings.TrimSpace(name))
	if name == "decimal" {
		return oid == types.T_decimal64 || oid == types.T_decimal128
	}
	return name == strings.ToLower(oid.String())
}

func validatePythonTypeDescriptor(descriptor PythonTypeDescriptor) error {
	typ := descriptor.Type()
	canonical, err := NewPythonTypeDescriptor(typ)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(canonical, descriptor) {
		return fmt.Errorf("descriptor is not canonical for %s", typ.String())
	}
	if _, err := descriptor.Fingerprint(); err != nil {
		return fmt.Errorf("descriptor has no valid Arrow physical type: %w", err)
	}
	return nil
}
func type2PlanType(typ types.Type) *plan.Type {
	return &plan.Type{Id: int32(typ.Oid), Width: typ.Width, Scale: typ.Scale, Charset: uint32(typ.Charset)}
}

func UdfArgTypeMatch(from []types.Type, to []types.T) (bool, int) {
	status, cost := tryToMatch(from, to)
	return status != matchFailed, cost
}
func UdfArgTypeCast(from []types.Type, to []types.T) []types.Type {
	castType := make([]types.Type, len(from))
	for i := range castType {
		if to[i] == from[i].Oid {
			castType[i] = from[i]
		} else {
			castType[i] = to[i].ToType()
			SetTargetScaleFromSource(&from[i], &castType[i])
		}
	}
	return castType
}
