package compile

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func TestPhyPlanJSON(t *testing.T) {
	// 创建示例数据
	operatorStats := &process.OperatorStats{
		OperatorName:          "ExampleOperator",
		CallCount:             10,
		TotalTimeConsumed:     5000,
		TotalWaitTimeConsumed: 2000,
		TotalMemorySize:       1024,
		TotalInputRows:        1000,
		TotalOutputRows:       950,
		TotalInputSize:        2048,
		TotalInputBlocks:      100,
		TotalOutputSize:       1900,
		TotalDiskIO:           300,
		TotalS3IOByte:         500,
		TotalS3InputCount:     5,
		TotalS3OutputCount:    4,
		TotalNetworkIO:        600,
		TotalScanTime:         1500,
		TotalInsertTime:       700,
	}

	//----------------------------------------------------operator---------------------------------------------------

	phyOperator3_0 := PhyOperator{
		OpName:  "TableScan",
		NodeIdx: 0,
		IsFirst: true,
		IsLast:  false,
		OpStats: operatorStats,
	}

	phyOperator3_1 := PhyOperator{
		OpName:   "Filter",
		NodeIdx:  0,
		IsFirst:  false,
		IsLast:   false,
		OpStats:  operatorStats,
		Children: []*PhyOperator{&phyOperator3_0},
	}

	phyOperator3_2 := PhyOperator{
		OpName:   "Projection",
		NodeIdx:  0,
		IsFirst:  false,
		IsLast:   true,
		OpStats:  operatorStats,
		Children: []*PhyOperator{&phyOperator3_1},
	}

	phyOperator3_3 := PhyOperator{
		OpName:   "Group",
		NodeIdx:  1,
		IsFirst:  true,
		IsLast:   false,
		OpStats:  operatorStats,
		Children: []*PhyOperator{&phyOperator3_2},
	}

	phyOperator3_4 := PhyOperator{
		OpName:  "Connect",
		NodeIdx: 1,
		IsFirst: false,
		IsLast:  false,
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
		IsFirst: false,
		IsLast:  false,
		OpStats: operatorStats,
	}

	phyOperator2_1 := PhyOperator{
		OpName:   "Projection",
		NodeIdx:  1,
		IsFirst:  false,
		IsLast:   true,
		OpStats:  operatorStats,
		Children: []*PhyOperator{&phyOperator2_0},
	}

	phyOperator2_2 := PhyOperator{
		OpName:   "projection",
		NodeIdx:  2,
		IsFirst:  true,
		IsLast:   false,
		OpStats:  operatorStats,
		Children: []*PhyOperator{&phyOperator2_1},
	}

	phyOperator2_3 := PhyOperator{
		OpName:  "Connect",
		NodeIdx: 2,
		IsFirst: false,
		IsLast:  false,
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
		IsFirst: false,
		IsLast:  true,
		OpStats: operatorStats,
	}

	phyOperator1_1 := PhyOperator{
		OpName:   "Output",
		NodeIdx:  -1,
		IsFirst:  false,
		IsLast:   false,
		OpStats:  operatorStats,
		Children: []*PhyOperator{&phyOperator1_0},
	}
	//---------------------------------------------------------scope---------------------------------------------------
	phyScope3 := PhyScope{
		Magic:        Merge,
		PreScopes:    []PhyScope{},
		RootOperator: &phyOperator3_4,
		//Pipeline:     []PhyOperator{phyOperator3_0, phyOperator3_1, phyOperator3_2, phyOperator3_3, phyOperator3_4},
		Receiver:   nil,
		DataSource: &PhySource{SchemaName: "schema", RelationName: "table", Attributes: []string{"col1", "col2"}},
	}

	phyScope2 := PhyScope{
		Magic:        Merge,
		PreScopes:    []PhyScope{phyScope3},
		RootOperator: &phyOperator2_3,
		//Pipeline:  []PhyOperator{phyOperator2_0, phyOperator2_1, phyOperator2_2, phyOperator2_3},
		Receiver: []PhyReceiver{
			{
				Idx:        0,
				RemoteUuid: "",
			},
		},
		DataSource: nil,
	}

	phyScope1 := PhyScope{
		Magic:        Normal,
		PreScopes:    []PhyScope{phyScope2},
		RootOperator: &phyOperator1_1,
		//Pipeline:  []PhyOperator{phyOperator1_0, phyOperator1_1},
		Receiver: []PhyReceiver{
			{
				Idx:        1,
				RemoteUuid: "",
			},
		},
		DataSource: nil,
	}

	//------------------------------------------------------------------------------------------------------------------

	phyPlan := PhyPlan{
		LocalScope: []PhyScope{phyScope1},
		Version:    "1.0.0",
	}

	// Convert to JSON
	jsonStr, err := PhyPlanToJSON(phyPlan)
	if err != nil {
		fmt.Printf("Error serializing to JSON: %s", err)
		return
	}
	fmt.Printf("JSON: %s\n", jsonStr)

	// Convert back from JSON
	phyPlanBack, err := JSONToPhyPlan(jsonStr)
	if err != nil {
		fmt.Printf("Error deserializing from JSON: %s", err)
		return
	}
	fmt.Printf("PhyPlan: %+v\n", phyPlanBack)

	//----------------------------------------------------
	jsonStr2, err := PhyPlanToJSON(phyPlanBack)
	if err != nil {
		fmt.Printf("Error serializing to JSON: %s", err)
		return
	}
	fmt.Printf("JSON2: %s\n", jsonStr2)

	/*
		// Convert to JSON
		jsonData, err := json.MarshalIndent(phyPlan, "", "  ")
		if err != nil {
			log.Fatalf("Error serializing to JSON: %s", err)
		}

		// print JSON string
		fmt.Println(string(jsonData))
	*/
}
