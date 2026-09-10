// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package function

import (
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strings"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/udf"
	pythonudf "github.com/matrixorigin/matrixone/pkg/udf/python"
)

type Udf struct {
	Body         string       `json:"body"`
	Language     string       `json:"language"`
	RetType      string       `json:"rettype"`
	Args         []*Arg       `json:"args"`
	Db           string       `json:"db"`
	ModifiedTime string       `json:"modified_time"`
	SQLMode      *string      `json:"-"`
	ArgsType     []types.Type `json:"-"`
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
}

// PythonRoutineBody is the single supported Python routine definition.  The
// catalog must persist mode and NULL policy with the source so execution does
// not infer either from a generic SQL function class bit.
type PythonRoutineBody struct {
	Handler           string                 `json:"handler"`
	Source            string                 `json:"source,omitempty"`
	Mode              string                 `json:"mode"`
	NullPolicy        string                 `json:"null_policy"`
	ABIContract       string                 `json:"abi_contract"`
	AdapterVersion    string                 `json:"adapter_version"`
	ArtifactDigest    string                 `json:"artifact_digest,omitempty"`
	EnvironmentDigest string                 `json:"environment_digest,omitempty"`
	SDKVersion        string                 `json:"sdk_version,omitempty"`
	ArgTypes          []PythonTypeDescriptor `json:"arg_types,omitempty"`
	ReturnType        *PythonTypeDescriptor  `json:"return_type,omitempty"`
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

// Validate checks fields that are independent of the catalog argument list.
// The argument count is checked by Udf.ValidatePythonTypeContract after the
// legacy catalog args column has been decoded.
func (body PythonRoutineBody) Validate() error {
	if body.Handler == "" || body.Source == "" {
		return fmt.Errorf("python routine handler and source are required")
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

func (u *Udf) GetPlanExpr() *plan.Expr {
	uid, _ := uuid.NewV7()
	data, _ := json.Marshal(&UdfWithContext{Udf: u, Context: map[string]string{"statement_id": uid.String()}})
	return &plan.Expr{Typ: *type2PlanType(types.T_text.ToType()), Expr: &plan.Expr_Lit{Lit: &plan.Literal{Isnull: false, Value: &plan.Literal_Sval{Sval: string(data)}}}}
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
	if u.Language == "python" {
		args := make([]types.Type, len(u.PythonArgTypes))
		for i, descriptor := range u.PythonArgTypes {
			args[i] = descriptor.Type()
		}
		return args
	}
	return u.ArgsType
}
func (u *Udf) GetRetType() types.Type {
	if u.Language == "python" {
		if u.PythonReturnType == nil {
			return types.Type{}
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
	if u.Language != "python" {
		return nil
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
	if u.Language != "python" {
		return nil
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

func validatePythonTypeDescriptor(descriptor PythonTypeDescriptor) error {
	typ := descriptor.Type()
	canonical, err := NewPythonTypeDescriptor(typ)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(canonical, descriptor) {
		return fmt.Errorf("descriptor is not canonical for %s", typ.String())
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
