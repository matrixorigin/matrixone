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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func TestExplainPhyPlan(t *testing.T) {
	operatorStats := &process.OperatorStats{
		OperatorName:     "ExampleOperator",
		CallNum:          10,
		TimeConsumed:     5000,
		WaitTimeConsumed: 2000,
		MemorySize:       1024,
		InputRows:        1000,
		OutputRows:       950,
		InputSize:        2048,
		InputBlocks:      0,
		OutputSize:       1900,
		ScanBytes:        0,
		NetworkIO:        600,
		//TotalScanTime:         1500,
		//TotalInsertTime:       0,
	}
	operatorStats.AddOpMetric(process.OpScanTime, 1500)

	//----------------------------------------------------operator---------------------------------------------------

	phyOperator3_0 := PhyOperator{
		OpName:  "TableScan",
		NodeIdx: 0,
		Status:  isFirstTrue | isLastFalse,
		OpStats: operatorStats,
	}

	phyOperator3_1 := PhyOperator{
		OpName:   "Filter",
		NodeIdx:  0,
		Status:   isFirstFalse | isLastFalse,
		OpStats:  operatorStats,
		Children: []*PhyOperator{&phyOperator3_0},
	}

	phyOperator3_2 := PhyOperator{
		OpName:   "Projection",
		NodeIdx:  0,
		Status:   isFirstFalse | isLastTrue,
		OpStats:  operatorStats,
		Children: []*PhyOperator{&phyOperator3_1},
	}

	phyOperator3_3 := PhyOperator{
		OpName:   "Group",
		NodeIdx:  1,
		Status:   isFirstTrue | isLastFalse,
		OpStats:  operatorStats,
		Children: []*PhyOperator{&phyOperator3_2},
	}

	phyOperator3_4 := PhyOperator{
		OpName:  "Connect",
		NodeIdx: 1,
		Status:  isFirstFalse | isLastFalse,
		DestReceiver: []PhyReceiver{
			{
				Idx:        0,
				RemoteUuid: "",
			},
		},
		OpStats:  operatorStats,
		Children: []*PhyOperator{&phyOperator3_3},
	}

	phyOperator2_0 := PhyOperator{
		OpName:  "Merge group",
		NodeIdx: 1,
		Status:  isFirstFalse | isLastFalse,
		OpStats: operatorStats,
	}

	phyOperator2_1 := PhyOperator{
		OpName:   "Projection",
		NodeIdx:  1,
		Status:   isFirstFalse | isLastTrue,
		OpStats:  operatorStats,
		Children: []*PhyOperator{&phyOperator2_0},
	}

	phyOperator2_2 := PhyOperator{
		OpName:   "projection",
		NodeIdx:  2,
		Status:   isFirstTrue | isLastFalse,
		OpStats:  operatorStats,
		Children: []*PhyOperator{&phyOperator2_1},
	}

	phyOperator2_3 := PhyOperator{
		OpName:  "Connect",
		NodeIdx: 2,
		Status:  isFirstFalse | isLastFalse,
		OpStats: operatorStats,
		DestReceiver: []PhyReceiver{
			{
				Idx:        1,
				RemoteUuid: "",
			},
		},
		Children: []*PhyOperator{&phyOperator2_2},
	}

	phyOperator1_0 := PhyOperator{
		OpName:  "Merge",
		NodeIdx: 2,
		Status:  isFirstFalse | isLastTrue,
		OpStats: operatorStats,
	}

	phyOperator1_1 := PhyOperator{
		OpName:   "Output",
		NodeIdx:  -1,
		Status:   isFirstFalse | isLastFalse,
		OpStats:  operatorStats,
		Children: []*PhyOperator{&phyOperator1_0},
	}
	//---------------------------------------------------------scope---------------------------------------------------
	phyScope3 := PhyScope{
		Magic:        "Merge",
		PreScopes:    []PhyScope{},
		RootOperator: &phyOperator3_4,
		Receiver:     nil,
		DataSource:   &PhySource{SchemaName: "schema", RelationName: "table", Attributes: []string{"col1", "col2"}},
	}

	phyScope2 := PhyScope{
		Magic:        "Merge",
		PreScopes:    []PhyScope{phyScope3},
		RootOperator: &phyOperator2_3,
		Receiver: []PhyReceiver{
			{
				Idx:        0,
				RemoteUuid: "",
			},
		},
		DataSource: nil,
	}

	phyScope1 := PhyScope{
		Magic:        "Normal",
		PreScopes:    []PhyScope{phyScope2},
		RootOperator: &phyOperator1_1,
		Receiver: []PhyReceiver{
			{
				Idx:        1,
				RemoteUuid: "",
			},
		},
		DataSource: nil,
	}

	phyPlan := NewPhyPlan()
	phyPlan.LocalScope = []PhyScope{phyScope1}
	phyPlan.RemoteScope = []PhyScope{phyScope1}

	statsInfo := new(statistic.StatsInfo)
	statsInfo.ParseStage.ParseDuration = 72872
	statsInfo.PlanStage.PlanDuration = 7544049
	statsInfo.PlanStage.BuildPlanStatsDuration = 142500
	statsInfo.CompileStage.CompileDuration = 59396
	statsInfo.CompileStage.CompileTableScanDuration = 260717
	statsInfo.CompileStage.CompileS3Request = statistic.S3Request{
		List:      0,
		Get:       2,
		Put:       1,
		Head:      1,
		Delete:    0,
		DeleteMul: 0,
	}

	statsInfo.PrepareRunStage.CompilePreRunOnceDuration = 49396
	statsInfo.PrepareRunStage.ScopePrepareDuration = 12000
	statsInfo.PrepareRunStage.BuildReaderDuration = 11000

	//------------------------------------------------------------------------------------------------------------------
	type args struct {
		plan   *PhyPlan
		stats  *statistic.StatsInfo
		option ExplainOption
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "test01",
			args: args{
				plan:   NewPhyPlan(),
				option: NormalOption,
			},
			want: "",
		},
		{
			name: "test02",
			args: args{
				plan:   phyPlan,
				stats:  statsInfo,
				option: AnalyzeOption,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ExplainPhyPlan(tt.args.plan, tt.args.stats, tt.args.option)
			t.Logf("%s", got)
		})
	}
}
