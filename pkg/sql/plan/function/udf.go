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

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
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
}

type UdfWithContext struct {
	*Udf    `json:"udf"`
	Context map[string]string `json:"context"`
}

// PythonRoutineBody is the single supported Python routine definition.  The
// catalog must persist mode and NULL policy with the source so execution does
// not infer either from a generic SQL function class bit.
type PythonRoutineBody struct {
	Handler           string `json:"handler"`
	Source            string `json:"source,omitempty"`
	Mode              string `json:"mode"`
	NullPolicy        string `json:"null_policy"`
	ABIContract       string `json:"abi_contract"`
	AdapterVersion    string `json:"adapter_version"`
	ArtifactDigest    string `json:"artifact_digest,omitempty"`
	EnvironmentDigest string `json:"environment_digest,omitempty"`
	SDKVersion        string `json:"sdk_version,omitempty"`
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
	result := make([]*plan.Type, len(u.ArgsType))
	for i, typ := range u.ArgsType {
		result[i] = type2PlanType(typ)
	}
	return result
}
func (u *Udf) GetRetPlanType() *plan.Type { return type2PlanType(u.GetRetType()) }
func (u *Udf) GetArgsType() []types.Type  { return u.ArgsType }
func (u *Udf) GetRetType() types.Type     { return types.Types[u.RetType].ToType() }
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
