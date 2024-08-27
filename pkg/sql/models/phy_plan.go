// Copyright 2024 Matrix Origin
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

package models

import (
	"encoding/json"

	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type PhyPlan struct {
	Version         string     `json:"version"`
	LocalScope      []PhyScope `json:"scope,omitempty"`
	RemoteScope     []PhyScope `json:"RemoteScope,omitempty"`
	S3IOInputCount  int64      `json:"S3IOInputCount"`
	S3IOOutputCount int64      `json:"S3IOOutputCount"`
}

type PhyScope struct {
	Magic        string        `json:"Magic"`
	Receiver     []PhyReceiver `json:"Receiver,omitempty"`
	DataSource   *PhySource    `json:"DataSource,omitempty"`
	PreScopes    []PhyScope    `json:"PreScopes,omitempty"`
	RootOperator *PhyOperator  `json:"RootOperator,omitempty"`
}

type PhyReceiver struct {
	Idx        int    `json:"Idx"`
	RemoteUuid string `json:"Uuid,omitempty"`
}

type PhySource struct {
	SchemaName   string   `json:"SchemaName"`
	RelationName string   `json:"TableName"`
	Attributes   []string `json:"Columns"`
}

type PhyOperator struct {
	OpName       string                 `json:"OpName"`
	NodeIdx      int                    `json:"NodeIdx"`
	Status       uint8                  `json:"Status"`
	DestReceiver []PhyReceiver          `json:"toMergeReceiver,omitempty"`
	OpStats      *process.OperatorStats `json:"OpStats,omitempty"`
	Children     []*PhyOperator         `json:"Children,omitempty"`
}

func NewPhyPlan() *PhyPlan {
	return &PhyPlan{
		// Assuming the version number is 1.0,
		Version:     "1.0",
		RemoteScope: []PhyScope{},
	}
}

func PhyPlanToJSON(p *PhyPlan) (string, error) {
	jsonData, err := json.MarshalIndent(p, "", "  ")
	if err != nil {
		return "", err
	}
	return string(jsonData), nil
}

func JSONToPhyPlan(jsonStr string) (PhyPlan, error) {
	var p PhyPlan
	err := json.Unmarshal([]byte(jsonStr), &p)
	if err != nil {
		return PhyPlan{}, err
	}
	return p, nil
}
