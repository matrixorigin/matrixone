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

package compile

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/models"
	"github.com/matrixorigin/matrixone/pkg/util"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	isFirstTrue = 1 << 0 // 0001 : isFirst = true
	isLastTrue  = 1 << 1 // 0010 : isLast = true

	isFirstFalse = 0 << 0 // 0000 : isFirst = false
	isLastFalse  = 0 << 1 // 0000 : isLast = false
)

func Test_processPhyScope(t *testing.T) {
	/*
		mysql> explain SELECT ROUND(SUM(distance), 2) AS total_distance from real_time_position WHERE time_stamp < 1694733800000;
		+----------------------------------------------------------------------------+
		| AP QUERY PLAN ON MULTICN(4 core)                                           |
		+----------------------------------------------------------------------------+
		| Project [2]                                                                |
		|   ->  Aggregate [1]                                                        |
		|         Aggregate Functions: sum(real_time_position.distance)              |
		|         ->  Table Scan [0]  on cloud_device.real_time_position             |
		|               Filter Cond: (real_time_position.time_stamp < 1694733800000) |
		+----------------------------------------------------------------------------+
		Version: 1.0, S3IOInputCount: 0, S3IOOutputCount: 0
		LOCAL SCOPES:
		Scope 1 (Magic: Merge, mcpu: 1, Receiver: [4])
		  Pipeline: └── Output (idx:-1, isFirst:false, isLast:false)
		                └── Projection (idx:2, isFirst:true, isLast:true)
		                    └── Projection (idx:1, isFirst:false, isLast:true)
		                        └── MergeGroup (idx:1, isFirst:false, isLast:false)
		                            └── Merge (idx:1, isFirst:false, isLast:false)
		  PreScopes: {
		    Scope 2 (Magic: Normal, mcpu: 4, Receiver: [0, 1, 2, 3])
		      Pipeline: └── Connector (idx:1, isFirst:false, isLast:false) to MergeReceiver 4
		                    └── MergeGroup (idx:1, isFirst:false, isLast:false)
		                        └── Merge (idx:1, isFirst:false, isLast:false)
		      PreScopes: {
		        Scope 3 (Magic: Normal, mcpu: 1, Receiver: [])
		          DataSource: cloud_device.real_time_position[time_stamp distance]
		          Pipeline: └── Connector (idx:0, isFirst:false, isLast:false) to MergeReceiver 0
		                        └── Group (idx:1, isFirst:true, isLast:false)
		                            └── Projection (idx:0, isFirst:false, isLast:true)
		                                └── Filter (idx:0, isFirst:false, isLast:false)
		                                    └── TableScan (idx:0, isFirst:true, isLast:false)
		        Scope 4 (Magic: Normal, mcpu: 1, Receiver: [])
		          DataSource: cloud_device.real_time_position[time_stamp distance]
		          Pipeline: └── Connector (idx:0, isFirst:false, isLast:false) to MergeReceiver 1
		                        └── Group (idx:1, isFirst:true, isLast:false)
		                            └── Projection (idx:0, isFirst:false, isLast:true)
		                                └── Filter (idx:0, isFirst:false, isLast:false)
		                                    └── TableScan (idx:0, isFirst:true, isLast:false)
		      }
		  }
	*/

	operatorStats := &process.OperatorStats{
		OperatorName:          "ExampleOperator",
		CallNum:               10,
		TotalTimeConsumed:     5000,
		TotalWaitTimeConsumed: 2000,
		TotalMemorySize:       1024,
		TotalInputRows:        1000,
		TotalOutputRows:       950,
		TotalInputSize:        2048,
		TotalInputBlocks:      0,
		TotalOutputSize:       1900,
		TotalScanBytes:        0,
		TotalNetworkIO:        600,
		//TotalScanTime:         1500,
		//TotalInsertTime:       0,
	}
	operatorStats.AddOpMetric(process.OpScanTime, 1500)
	operatorStats.AddOpMetric(process.OpInsertTime, 2500)
	operatorStats.AddOpMetric(process.OpIncrementTime, 3500)

	phyOperator1_0 := models.PhyOperator{
		OpName:  "Merge",
		NodeIdx: 1,
		Status:  isFirstFalse | isLastFalse,
		OpStats: operatorStats,
	}

	phyOperator1_1 := models.PhyOperator{
		OpName:   "MergeGroup",
		NodeIdx:  1,
		Status:   isFirstFalse | isLastFalse,
		OpStats:  operatorStats,
		Children: []*models.PhyOperator{&phyOperator1_0},
	}

	phyOperator1_2 := models.PhyOperator{
		OpName:   "Projection",
		NodeIdx:  1,
		Status:   isFirstFalse | isLastTrue,
		OpStats:  operatorStats,
		Children: []*models.PhyOperator{&phyOperator1_1},
	}

	phyOperator1_3 := models.PhyOperator{
		OpName:   "Projection",
		NodeIdx:  1,
		Status:   isFirstTrue | isLastTrue,
		OpStats:  operatorStats,
		Children: []*models.PhyOperator{&phyOperator1_2},
	}

	phyOperator1_4 := models.PhyOperator{
		OpName:   "Output",
		NodeIdx:  -1,
		Status:   isFirstFalse | isLastFalse,
		OpStats:  operatorStats,
		Children: []*models.PhyOperator{&phyOperator1_3},
	}

	phyScope1 := models.PhyScope{
		Magic: "Merge",
		Receiver: []models.PhyReceiver{
			{
				Idx:        4,
				RemoteUuid: "",
			},
		},
		RootOperator: &phyOperator1_4,
	}

	//---------------------------------------------------------------------------------
	phyOperator2_0 := models.PhyOperator{
		OpName:  "Merge",
		NodeIdx: 1,
		Status:  isFirstFalse | isLastFalse,
		OpStats: operatorStats,
	}

	phyOperator2_1 := models.PhyOperator{
		OpName:   "MergeGroup",
		NodeIdx:  1,
		Status:   isFirstFalse | isLastFalse,
		OpStats:  operatorStats,
		Children: []*models.PhyOperator{&phyOperator2_0},
	}

	phyOperator2_2 := models.PhyOperator{
		OpName:  "Connect",
		NodeIdx: 1,
		Status:  isFirstFalse | isLastFalse,
		DestReceiver: []models.PhyReceiver{
			{
				Idx:        4,
				RemoteUuid: "",
			},
		},
		OpStats:  operatorStats,
		Children: []*models.PhyOperator{&phyOperator2_1},
	}

	phyScope2 := models.PhyScope{
		Magic: "Normal",
		Receiver: []models.PhyReceiver{
			{
				Idx:        0,
				RemoteUuid: "",
			},
			{
				Idx:        1,
				RemoteUuid: "",
			},
		},
		RootOperator: &phyOperator2_2,
	}
	//---------------------------------------------------------------------------------

	phyOperator3_0 := models.PhyOperator{
		OpName:  "TableScan",
		NodeIdx: 0,
		Status:  isFirstTrue | isLastFalse,
		OpStats: operatorStats,
	}

	phyOperator3_1 := models.PhyOperator{
		OpName:   "Filter",
		NodeIdx:  0,
		Status:   isFirstFalse | isLastFalse,
		OpStats:  operatorStats,
		Children: []*models.PhyOperator{&phyOperator3_0},
	}

	phyOperator3_2 := models.PhyOperator{
		OpName:   "Projection",
		NodeIdx:  0,
		Status:   isFirstFalse | isLastTrue,
		OpStats:  operatorStats,
		Children: []*models.PhyOperator{&phyOperator3_1},
	}

	phyOperator3_3 := models.PhyOperator{
		OpName:   "Group",
		NodeIdx:  1,
		Status:   isFirstTrue | isLastFalse,
		OpStats:  operatorStats,
		Children: []*models.PhyOperator{&phyOperator3_2},
	}

	phyOperator3_4 := models.PhyOperator{
		OpName:  "Connect",
		NodeIdx: 1,
		Status:  isFirstFalse | isLastFalse,
		DestReceiver: []models.PhyReceiver{
			{
				Idx:        0,
				RemoteUuid: "",
			},
		},
		OpStats:  operatorStats,
		Children: []*models.PhyOperator{&phyOperator3_3},
	}

	phyScope3 := models.PhyScope{
		Magic:        "Normal",
		PreScopes:    []models.PhyScope{},
		RootOperator: &phyOperator3_4,
	}
	//----------------------------------------------------------------------------
	phyOperator4_0 := models.PhyOperator{
		OpName:  "TableScan",
		NodeIdx: 0,
		Status:  isFirstTrue | isLastFalse,
		OpStats: operatorStats,
	}

	phyOperator4_1 := models.PhyOperator{
		OpName:   "Filter",
		NodeIdx:  0,
		Status:   isFirstFalse | isLastFalse,
		OpStats:  operatorStats,
		Children: []*models.PhyOperator{&phyOperator4_0},
	}

	phyOperator4_2 := models.PhyOperator{
		OpName:   "Projection",
		NodeIdx:  0,
		Status:   isFirstFalse | isLastTrue,
		OpStats:  operatorStats,
		Children: []*models.PhyOperator{&phyOperator4_1},
	}

	phyOperator4_3 := models.PhyOperator{
		OpName:   "Group",
		NodeIdx:  1,
		Status:   isFirstTrue | isLastFalse,
		OpStats:  operatorStats,
		Children: []*models.PhyOperator{&phyOperator4_2},
	}

	phyOperator4_4 := models.PhyOperator{
		OpName:  "Connect",
		NodeIdx: 1,
		Status:  isFirstFalse | isLastFalse,
		DestReceiver: []models.PhyReceiver{
			{
				Idx:        0,
				RemoteUuid: "",
			},
		},
		OpStats:  operatorStats,
		Children: []*models.PhyOperator{&phyOperator4_3},
	}

	phyScope4 := models.PhyScope{
		Magic:        "Normal",
		RootOperator: &phyOperator4_4,
	}

	phyScope2.PreScopes = []models.PhyScope{phyScope3, phyScope4}
	phyScope1.PreScopes = []models.PhyScope{phyScope2}

	type args struct {
		scope models.PhyScope
		nodes []*plan.Node
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "test01",
			args: args{
				scope: phyScope1,
				nodes: []*plan.Node{
					{
						NodeId:      0,
						NodeType:    plan.Node_TABLE_SCAN,
						AnalyzeInfo: new(plan.AnalyzeInfo),
						Children:    []int32{},
					},
					{
						NodeId:      1,
						NodeType:    plan.Node_AGG,
						AnalyzeInfo: new(plan.AnalyzeInfo),
						Children:    []int32{0},
					},
					{
						NodeId:      2,
						NodeType:    plan.Node_PROJECT,
						AnalyzeInfo: new(plan.AnalyzeInfo),
						Children:    []int32{1},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			phyPlan := models.NewPhyPlan()
			phyPlan.LocalScope = append(phyPlan.LocalScope, tt.args.scope)
			explainPhyPlan := models.ExplainPhyPlan(phyPlan, models.NormalOption)
			t.Logf("%s", explainPhyPlan)
			processPhyScope(&tt.args.scope, tt.args.nodes)
		})
	}
}

func Test_explainGlobalResources(t *testing.T) {
	operatorStats := &process.OperatorStats{
		OperatorName:          "ExampleOperator",
		CallNum:               10,
		TotalTimeConsumed:     5000,
		TotalWaitTimeConsumed: 2000,
		TotalMemorySize:       1024,
		TotalInputRows:        1000,
		TotalOutputRows:       950,
		TotalInputSize:        2048,
		TotalInputBlocks:      0,
		TotalOutputSize:       1900,
		TotalScanBytes:        0,
		TotalNetworkIO:        600,
		//TotalScanTime:         1500,
		//TotalInsertTime:       0,
	}
	operatorStats.AddOpMetric(process.OpScanTime, 1500)
	operatorStats.AddOpMetric(process.OpInsertTime, 2500)
	operatorStats.AddOpMetric(process.OpIncrementTime, 3500)

	phyOperator1_0 := models.PhyOperator{
		OpName:  "Merge",
		NodeIdx: 1,
		Status:  isFirstFalse | isLastFalse,
		OpStats: operatorStats,
	}

	phyOperator1_1 := models.PhyOperator{
		OpName:   "MergeGroup",
		NodeIdx:  1,
		Status:   isFirstFalse | isLastFalse,
		OpStats:  operatorStats,
		Children: []*models.PhyOperator{&phyOperator1_0},
	}

	phyOperator1_2 := models.PhyOperator{
		OpName:   "Projection",
		NodeIdx:  1,
		Status:   isFirstFalse | isLastTrue,
		OpStats:  operatorStats,
		Children: []*models.PhyOperator{&phyOperator1_1},
	}

	phyOperator1_3 := models.PhyOperator{
		OpName:   "Projection",
		NodeIdx:  1,
		Status:   isFirstTrue | isLastTrue,
		OpStats:  operatorStats,
		Children: []*models.PhyOperator{&phyOperator1_2},
	}

	phyOperator1_4 := models.PhyOperator{
		OpName:   "Output",
		NodeIdx:  -1,
		Status:   isFirstFalse | isLastFalse,
		OpStats:  operatorStats,
		Children: []*models.PhyOperator{&phyOperator1_3},
	}

	phyScope1 := models.PhyScope{
		Magic: "Merge",
		Receiver: []models.PhyReceiver{
			{
				Idx:        4,
				RemoteUuid: "",
			},
		},
		RootOperator: &phyOperator1_4,
	}

	//---------------------------------------------------------------------------------
	phyOperator2_0 := models.PhyOperator{
		OpName:  "Merge",
		NodeIdx: 1,
		Status:  isFirstFalse | isLastFalse,
		OpStats: operatorStats,
	}

	phyOperator2_1 := models.PhyOperator{
		OpName:   "MergeGroup",
		NodeIdx:  1,
		Status:   isFirstFalse | isLastFalse,
		OpStats:  operatorStats,
		Children: []*models.PhyOperator{&phyOperator2_0},
	}

	phyOperator2_2 := models.PhyOperator{
		OpName:  "Connect",
		NodeIdx: 1,
		Status:  isFirstFalse | isLastFalse,
		DestReceiver: []models.PhyReceiver{
			{
				Idx:        4,
				RemoteUuid: "",
			},
		},
		OpStats:  operatorStats,
		Children: []*models.PhyOperator{&phyOperator2_1},
	}

	phyScope2 := models.PhyScope{
		Magic: "Normal",
		Receiver: []models.PhyReceiver{
			{
				Idx:        0,
				RemoteUuid: "",
			},
			{
				Idx:        1,
				RemoteUuid: "",
			},
		},
		RootOperator: &phyOperator2_2,
	}
	//---------------------------------------------------------------------------------

	phyOperator3_0 := models.PhyOperator{
		OpName:  "TableScan",
		NodeIdx: 0,
		Status:  isFirstTrue | isLastFalse,
		OpStats: operatorStats,
	}

	phyOperator3_1 := models.PhyOperator{
		OpName:   "Filter",
		NodeIdx:  0,
		Status:   isFirstFalse | isLastFalse,
		OpStats:  operatorStats,
		Children: []*models.PhyOperator{&phyOperator3_0},
	}

	phyOperator3_2 := models.PhyOperator{
		OpName:   "Projection",
		NodeIdx:  0,
		Status:   isFirstFalse | isLastTrue,
		OpStats:  operatorStats,
		Children: []*models.PhyOperator{&phyOperator3_1},
	}

	phyOperator3_3 := models.PhyOperator{
		OpName:   "Group",
		NodeIdx:  1,
		Status:   isFirstTrue | isLastFalse,
		OpStats:  operatorStats,
		Children: []*models.PhyOperator{&phyOperator3_2},
	}

	phyOperator3_4 := models.PhyOperator{
		OpName:  "Connect",
		NodeIdx: 1,
		Status:  isFirstFalse | isLastFalse,
		DestReceiver: []models.PhyReceiver{
			{
				Idx:        0,
				RemoteUuid: "",
			},
		},
		OpStats:  operatorStats,
		Children: []*models.PhyOperator{&phyOperator3_3},
	}

	phyScope3 := models.PhyScope{
		Magic:        "Normal",
		PreScopes:    []models.PhyScope{},
		RootOperator: &phyOperator3_4,
	}
	//----------------------------------------------------------------------------
	phyOperator4_0 := models.PhyOperator{
		OpName:  "TableScan",
		NodeIdx: 0,
		Status:  isFirstTrue | isLastFalse,
		OpStats: operatorStats,
	}

	phyOperator4_1 := models.PhyOperator{
		OpName:   "Filter",
		NodeIdx:  0,
		Status:   isFirstFalse | isLastFalse,
		OpStats:  operatorStats,
		Children: []*models.PhyOperator{&phyOperator4_0},
	}

	phyOperator4_2 := models.PhyOperator{
		OpName:   "Projection",
		NodeIdx:  0,
		Status:   isFirstFalse | isLastTrue,
		OpStats:  operatorStats,
		Children: []*models.PhyOperator{&phyOperator4_1},
	}

	phyOperator4_3 := models.PhyOperator{
		OpName:   "Group",
		NodeIdx:  1,
		Status:   isFirstTrue | isLastFalse,
		OpStats:  operatorStats,
		Children: []*models.PhyOperator{&phyOperator4_2},
	}

	phyOperator4_4 := models.PhyOperator{
		OpName:  "Connect",
		NodeIdx: 1,
		Status:  isFirstFalse | isLastFalse,
		DestReceiver: []models.PhyReceiver{
			{
				Idx:        0,
				RemoteUuid: "",
			},
		},
		OpStats:  operatorStats,
		Children: []*models.PhyOperator{&phyOperator4_3},
	}

	phyScope4 := models.PhyScope{
		Magic:        "Normal",
		RootOperator: &phyOperator4_4,
	}

	phyScope2.PreScopes = []models.PhyScope{phyScope3, phyScope4}
	phyScope1.PreScopes = []models.PhyScope{phyScope2}

	type args struct {
		queryResult *util.RunResult
		statsInfo   *statistic.StatsInfo
		anal        *AnalyzeModule
		option      *ExplainOption
		buffer      *bytes.Buffer
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "test01",
			args: args{
				queryResult: &util.RunResult{
					AffectRows: 1,
				},
				statsInfo: &statistic.StatsInfo{
					ParseDuration:               72872,
					PlanDuration:                7544049,
					CompileDuration:             59396,
					BuildReaderDuration:         260717,
					BuildPlanStatsDuration:      142500736,
					BuildPlanResolveVarDuration: 605813,
				},
				anal: &AnalyzeModule{
					phyPlan: &models.PhyPlan{},
				},
				option: &ExplainOption{
					Analyze: true,
				},
				buffer: bytes.NewBuffer(make([]byte, 0, 300)),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			phyPlan := models.NewPhyPlan()
			phyPlan.LocalScope = append(phyPlan.LocalScope, phyScope1)
			tt.args.anal.phyPlan = phyPlan
			explainGlobalResources(tt.args.queryResult, tt.args.statsInfo, tt.args.anal, tt.args.option, tt.args.buffer)
			t.Logf("%s", tt.args.buffer.String())
		})
	}
}
