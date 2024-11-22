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

package function

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
)

func initMoExplainPhyTestCase() []tcTemp {
	exec_plan_json := `{
  "steps" : [ {
    "graphData" : {
      "nodes" : [ {
        "id" : "2",
        "name" : "Project",
        "title" : "round(SUM(real_time_position.distance), 2)",
        "labels" : [ {
          "name" : "List of expressions",
          "value" : [ "round(SUM(real_time_position.distance), 2)" ]
        } ],
        "statistics" : {
          "Time" : [ {
            "name" : "Time Consumed",
            "value" : 15439,
            "unit" : "ns"
          }, {
            "name" : "Wait Time",
            "value" : 0,
            "unit" : "ns"
          }, {
            "name" : "Scan Time",
            "value" : 0,
            "unit" : "ns"
          }, {
            "name" : "Insert Time",
            "value" : 0,
            "unit" : "ns"
          } ],
          "Memory" : [ {
            "name" : "Memory Size",
            "value" : 8,
            "unit" : "byte"
          } ],
          "Throughput" : [ {
            "name" : "Input Rows",
            "value" : 1,
            "unit" : "count"
          }, {
            "name" : "Output Rows",
            "value" : 1,
            "unit" : "count"
          }, {
            "name" : "Input Size",
            "value" : 8,
            "unit" : "byte"
          }, {
            "name" : "Output Size",
            "value" : 8,
            "unit" : "byte"
          } ],
          "IO" : [ {
            "name" : "Disk IO",
            "value" : 0,
            "unit" : "byte"
          }, {
            "name" : "Scan Bytes",
            "value" : 0,
            "unit" : "byte"
          }, {
            "name" : "S3 List Count",
            "value" : 0,
            "unit" : "count"
          }, {
            "name" : "S3 Head Count",
            "value" : 0,
            "unit" : "count"
          }, {
            "name" : "S3 Put Count",
            "value" : 0,
            "unit" : "count"
          }, {
            "name" : "S3 Get Count",
            "value" : 0,
            "unit" : "count"
          }, {
            "name" : "S3 Delete Count",
            "value" : 0,
            "unit" : "count"
          }, {
            "name" : "S3 DeleteMul Count",
            "value" : 0,
            "unit" : "count"
          } ],
          "Network" : [ {
            "name" : "Network",
            "value" : 0,
            "unit" : "byte"
          } ]
        },
        "stats" : {
          "blocknum" : 7,
          "outcnt" : 1,
          "cost" : 1,
          "hashmapsize" : 0,
          "rowsize" : 100
        },
        "totalStats" : {
          "name" : "Time spent",
          "value" : 15439,
          "unit" : "ns"
        }
      }, {
        "id" : "1",
        "name" : "Aggregate",
        "title" : "sum(real_time_position.distance)",
        "labels" : [ {
          "name" : "Aggregate functions",
          "value" : [ "sum(real_time_position.distance)" ]
        } ],
        "statistics" : {
          "Time" : [ {
            "name" : "Time Consumed",
            "value" : 1517249,
            "unit" : "ns"
          }, {
            "name" : "Wait Time",
            "value" : 81917495,
            "unit" : "ns"
          }, {
            "name" : "Scan Time",
            "value" : 0,
            "unit" : "ns"
          }, {
            "name" : "Insert Time",
            "value" : 0,
            "unit" : "ns"
          } ],
          "Memory" : [ {
            "name" : "Memory Size",
            "value" : 16,
            "unit" : "byte"
          } ],
          "Throughput" : [ {
            "name" : "Input Rows",
            "value" : 69531,
            "unit" : "count"
          }, {
            "name" : "Output Rows",
            "value" : 1,
            "unit" : "count"
          }, {
            "name" : "Input Size",
            "value" : 556248,
            "unit" : "byte"
          }, {
            "name" : "Output Size",
            "value" : 8,
            "unit" : "byte"
          } ],
          "IO" : [ {
            "name" : "Disk IO",
            "value" : 0,
            "unit" : "byte"
          }, {
            "name" : "Scan Bytes",
            "value" : 0,
            "unit" : "byte"
          }, {
            "name" : "S3 List Count",
            "value" : 0,
            "unit" : "count"
          }, {
            "name" : "S3 Head Count",
            "value" : 0,
            "unit" : "count"
          }, {
            "name" : "S3 Put Count",
            "value" : 0,
            "unit" : "count"
          }, {
            "name" : "S3 Get Count",
            "value" : 0,
            "unit" : "count"
          }, {
            "name" : "S3 Delete Count",
            "value" : 0,
            "unit" : "count"
          }, {
            "name" : "S3 DeleteMul Count",
            "value" : 0,
            "unit" : "count"
          } ],
          "Network" : [ {
            "name" : "Network",
            "value" : 0,
            "unit" : "byte"
          } ]
        },
        "stats" : {
          "blocknum" : 7,
          "outcnt" : 1,
          "cost" : 5000000,
          "hashmapsize" : 1,
          "rowsize" : 100
        },
        "totalStats" : {
          "name" : "Time spent",
          "value" : 1517249,
          "unit" : "ns"
        }
      }, {
        "id" : "0",
        "name" : "Table Scan",
        "title" : "cloud_device.real_time_position",
        "labels" : [ {
          "name" : "Full table name",
          "value" : "cloud_device.real_time_position"
        }, {
          "name" : "Columns",
          "value" : [ "time_stamp", "distance" ]
        }, {
          "name" : "Total columns",
          "value" : 9
        }, {
          "name" : "Scan columns",
          "value" : 2
        }, {
          "name" : "Filter conditions",
          "value" : [ "real_time_position.time_stamp BETWEEN 1694733800000 AND 1694750000000" ]
        } ],
        "statistics" : {
          "Time" : [ {
            "name" : "Time Consumed",
            "value" : 303118705,
            "unit" : "ns"
          }, {
            "name" : "Wait Time",
            "value" : 0,
            "unit" : "ns"
          }, {
            "name" : "Scan Time",
            "value" : 0,
            "unit" : "ns"
          }, {
            "name" : "Insert Time",
            "value" : 0,
            "unit" : "ns"
          } ],
          "Memory" : [ {
            "name" : "Memory Size",
            "value" : 5623936,
            "unit" : "byte"
          } ],
          "Throughput" : [ {
            "name" : "Input Rows",
            "value" : 5000000,
            "unit" : "count"
          }, {
            "name" : "Output Rows",
            "value" : 69531,
            "unit" : "count"
          }, {
            "name" : "Input Size",
            "value" : 80000000,
            "unit" : "byte"
          }, {
            "name" : "Output Size",
            "value" : 556248,
            "unit" : "byte"
          } ],
          "IO" : [ {
            "name" : "Disk IO",
            "value" : 66715122,
            "unit" : "byte"
          }, {
            "name" : "Scan Bytes",
            "value" : 80000000,
            "unit" : "byte"
          }, {
            "name" : "S3 List Count",
            "value" : 0,
            "unit" : "count"
          }, {
            "name" : "S3 Head Count",
            "value" : 0,
            "unit" : "count"
          }, {
            "name" : "S3 Put Count",
            "value" : 0,
            "unit" : "count"
          }, {
            "name" : "S3 Get Count",
            "value" : 0,
            "unit" : "count"
          }, {
            "name" : "S3 Delete Count",
            "value" : 0,
            "unit" : "count"
          }, {
            "name" : "S3 DeleteMul Count",
            "value" : 0,
            "unit" : "count"
          } ],
          "Network" : [ {
            "name" : "Network",
            "value" : 0,
            "unit" : "byte"
          } ]
        },
        "stats" : {
          "blocknum" : 612,
          "outcnt" : 55803.422208864096,
          "cost" : 5000000,
          "hashmapsize" : 1,
          "rowsize" : 0
        },
        "totalStats" : {
          "name" : "Time spent",
          "value" : 303118705,
          "unit" : "ns"
        }
      } ],
      "edges" : [ {
        "id" : "E1",
        "src" : "1",
        "dst" : "2",
        "output" : 1,
        "unit" : "count"
      }, {
        "id" : "E0",
        "src" : "0",
        "dst" : "1",
        "output" : 69531,
        "unit" : "count"
      } ],
      "labels" : [ ],
      "global" : {
        "statistics" : {
          "Time" : [ {
            "name" : "Time Consumed",
            "value" : 304651393,
            "unit" : "ns"
          }, {
            "name" : "Wait Time",
            "value" : 81917495,
            "unit" : "ns"
          } ],
          "Memory" : [ {
            "name" : "Memory Size",
            "value" : 5623960,
            "unit" : "byte"
          } ],
          "Throughput" : [ {
            "name" : "Input Rows",
            "value" : 5069532,
            "unit" : "count"
          }, {
            "name" : "Output Rows",
            "value" : 69533,
            "unit" : "count"
          }, {
            "name" : "Input Size",
            "value" : 80556256,
            "unit" : "byte"
          }, {
            "name" : "Output Size",
            "value" : 556264,
            "unit" : "byte"
          } ],
          "IO" : [ {
            "name" : "Disk IO",
            "value" : 66715122,
            "unit" : "byte"
          }, {
            "name" : "Scan Bytes",
            "value" : 80000000,
            "unit" : "byte"
          }, {
            "name" : "S3 IO Input Count",
            "value" : 0,
            "unit" : "count"
          }, {
            "name" : "S3 IO Output Count",
            "value" : 0,
            "unit" : "count"
          }, {
            "name" : "S3 List Count",
            "value" : 0,
            "unit" : "count"
          }, {
            "name" : "S3 Head Count",
            "value" : 0,
            "unit" : "count"
          }, {
            "name" : "S3 Put Count",
            "value" : 0,
            "unit" : "count"
          }, {
            "name" : "S3 Get Count",
            "value" : 0,
            "unit" : "count"
          }, {
            "name" : "S3 Delete Count",
            "value" : 0,
            "unit" : "count"
          }, {
            "name" : "S3 DeleteMul Count",
            "value" : 0,
            "unit" : "count"
          } ],
          "Network" : [ {
            "name" : "Network",
            "value" : 0,
            "unit" : "byte"
          } ]
        },
        "totalStats" : {
          "name" : "Time spent",
          "value" : 304651393,
          "unit" : "ns"
        }
      }
    },
    "step" : 0,
    "description" : "",
    "state" : "success",
    "stats" : { }
  } ],
  "code" : 0,
  "message" : "",
  "uuid" : "0192bd38-73ad-7c57-a863-008010678b65",
  "PhyPlan" : {
    "version" : "1.0",
    "scope" : [ {
      "Magic" : "Merge",
      "Mcpu" : 1,
      "Receiver" : [ {
        "Idx" : 0
      } ],
      "PreScopes" : [ {
        "Magic" : "Remote",
        "Mcpu" : 4,
        "DataSource" : {
          "SchemaName" : "cloud_device",
          "TableName" : "real_time_position",
          "Columns" : [ "time_stamp", "distance" ]
        },
        "PreScopes" : [ {
          "Magic" : "Normal",
          "PreScopes" : [ {
            "Magic" : "Normal",
            "Mcpu" : 1,
            "DataSource" : {
              "SchemaName" : "cloud_device",
              "TableName" : "real_time_position",
              "Columns" : [ "time_stamp", "distance" ]
            },
            "RootOperator" : {
              "OpName" : "Connector",
              "NodeIdx" : 1,
              "Status" : 0,
              "toMergeReceiver" : [ {
                "Idx" : 0
              } ],
              "OpStats" : {
                "CallCount" : 2,
                "TimeConsumed" : 22677
              },
              "Children" : [ {
                "OpName" : "Group",
                "NodeIdx" : 1,
                "Status" : 1,
                "OpStats" : {
                  "CallCount" : 2,
                  "TimeConsumed" : 338784,
                  "InputRows" : 15593,
                  "InputSize" : 124744
                },
                "Children" : [ {
                  "OpName" : "Projection",
                  "NodeIdx" : 0,
                  "Status" : 2,
                  "OpStats" : {
                    "CallCount" : 153,
                    "TimeConsumed" : 61454,
                    "MemorySize" : 29800,
                    "OutputRows" : 15593,
                    "OutputSize" : 124744
                  },
                  "Children" : [ {
                    "OpName" : "Filter",
                    "NodeIdx" : 0,
                    "Status" : 0,
                    "OpStats" : {
                      "CallCount" : 153,
                      "TimeConsumed" : 16747009,
                      "MemorySize" : 1245184
                    },
                    "Children" : [ {
                      "OpName" : "TableScan",
                      "NodeIdx" : 0,
                      "Status" : 1,
                      "OpStats" : {
                        "CallCount" : 153,
                        "TimeConsumed" : 58472333,
                        "MemorySize" : 131072,
                        "InputRows" : 1245184,
                        "InputSize" : 19922944,
                        "DiskIO" : 16491709
                      }
                    } ]
                  } ]
                } ]
              } ]
            },
            "PrepareTimeConsumed" : 561
          }, {
            "Magic" : "Normal",
            "Mcpu" : 1,
            "DataSource" : {
              "SchemaName" : "cloud_device",
              "TableName" : "real_time_position",
              "Columns" : [ "time_stamp", "distance" ]
            },
            "RootOperator" : {
              "OpName" : "Connector",
              "NodeIdx" : 1,
              "Status" : 0,
              "toMergeReceiver" : [ {
                "Idx" : 0
              } ],
              "OpStats" : {
                "CallCount" : 2,
                "TimeConsumed" : 9887
              },
              "Children" : [ {
                "OpName" : "Group",
                "NodeIdx" : 1,
                "Status" : 1,
                "OpStats" : {
                  "CallCount" : 2,
                  "TimeConsumed" : 319777,
                  "InputRows" : 15405,
                  "InputSize" : 123240
                },
                "Children" : [ {
                  "OpName" : "Projection",
                  "NodeIdx" : 0,
                  "Status" : 2,
                  "OpStats" : {
                    "CallCount" : 154,
                    "TimeConsumed" : 66538,
                    "MemorySize" : 18864,
                    "OutputRows" : 15405,
                    "OutputSize" : 123240
                  },
                  "Children" : [ {
                    "OpName" : "Filter",
                    "NodeIdx" : 0,
                    "Status" : 0,
                    "OpStats" : {
                      "CallCount" : 154,
                      "TimeConsumed" : 18563940,
                      "MemorySize" : 1253376
                    },
                    "Children" : [ {
                      "OpName" : "TableScan",
                      "NodeIdx" : 0,
                      "Status" : 1,
                      "OpStats" : {
                        "CallCount" : 154,
                        "TimeConsumed" : 61209774,
                        "MemorySize" : 131072,
                        "InputRows" : 1253376,
                        "InputSize" : 20054016,
                        "DiskIO" : 16797879
                      }
                    } ]
                  } ]
                } ]
              } ]
            },
            "PrepareTimeConsumed" : 806
          }, {
            "Magic" : "Normal",
            "Mcpu" : 1,
            "DataSource" : {
              "SchemaName" : "cloud_device",
              "TableName" : "real_time_position",
              "Columns" : [ "time_stamp", "distance" ]
            },
            "RootOperator" : {
              "OpName" : "Connector",
              "NodeIdx" : 1,
              "Status" : 0,
              "toMergeReceiver" : [ {
                "Idx" : 0
              } ],
              "OpStats" : {
                "CallCount" : 2,
                "TimeConsumed" : 2950
              },
              "Children" : [ {
                "OpName" : "Group",
                "NodeIdx" : 1,
                "Status" : 1,
                "OpStats" : {
                  "CallCount" : 2,
                  "TimeConsumed" : 396141,
                  "InputRows" : 19167,
                  "InputSize" : 153336
                },
                "Children" : [ {
                  "OpName" : "Projection",
                  "NodeIdx" : 0,
                  "Status" : 2,
                  "OpStats" : {
                    "CallCount" : 154,
                    "TimeConsumed" : 82273,
                    "MemorySize" : 24624,
                    "OutputRows" : 19167,
                    "OutputSize" : 153336
                  },
                  "Children" : [ {
                    "OpName" : "Filter",
                    "NodeIdx" : 0,
                    "Status" : 0,
                    "OpStats" : {
                      "CallCount" : 154,
                      "TimeConsumed" : 17764543,
                      "MemorySize" : 1253376
                    },
                    "Children" : [ {
                      "OpName" : "TableScan",
                      "NodeIdx" : 0,
                      "Status" : 1,
                      "OpStats" : {
                        "CallCount" : 154,
                        "TimeConsumed" : 58087865,
                        "MemorySize" : 131072,
                        "InputRows" : 1253376,
                        "InputSize" : 20054016,
                        "DiskIO" : 16717018
                      }
                    } ]
                  } ]
                } ]
              } ]
            },
            "PrepareTimeConsumed" : 827
          }, {
            "Magic" : "Normal",
            "Mcpu" : 1,
            "DataSource" : {
              "SchemaName" : "cloud_device",
              "TableName" : "real_time_position",
              "Columns" : [ "time_stamp", "distance" ]
            },
            "RootOperator" : {
              "OpName" : "Connector",
              "NodeIdx" : 1,
              "Status" : 0,
              "toMergeReceiver" : [ {
                "Idx" : 0
              } ],
              "OpStats" : {
                "CallCount" : 2,
                "TimeConsumed" : 6821
              },
              "Children" : [ {
                "OpName" : "Group",
                "NodeIdx" : 1,
                "Status" : 1,
                "OpStats" : {
                  "CallCount" : 2,
                  "TimeConsumed" : 374224,
                  "InputRows" : 19366,
                  "InputSize" : 154928
                },
                "Children" : [ {
                  "OpName" : "Projection",
                  "NodeIdx" : 0,
                  "Status" : 2,
                  "OpStats" : {
                    "CallCount" : 154,
                    "TimeConsumed" : 59683,
                    "MemorySize" : 26360,
                    "OutputRows" : 19366,
                    "OutputSize" : 154928
                  },
                  "Children" : [ {
                    "OpName" : "Filter",
                    "NodeIdx" : 0,
                    "Status" : 0,
                    "OpStats" : {
                      "CallCount" : 154,
                      "TimeConsumed" : 17421140,
                      "MemorySize" : 1248064
                    },
                    "Children" : [ {
                      "OpName" : "TableScan",
                      "NodeIdx" : 0,
                      "Status" : 1,
                      "OpStats" : {
                        "CallCount" : 154,
                        "TimeConsumed" : 54582153,
                        "MemorySize" : 131072,
                        "InputRows" : 1248064,
                        "InputSize" : 19969024,
                        "DiskIO" : 16708516
                      }
                    } ]
                  } ]
                } ]
              } ]
            },
            "PrepareTimeConsumed" : 178
          } ],
          "PrepareTimeConsumed" : 8026
        } ],
        "RootOperator" : {
          "OpName" : "Connector",
          "NodeIdx" : 1,
          "Status" : 0,
          "toMergeReceiver" : [ {
            "Idx" : 0
          } ],
          "Children" : [ {
            "OpName" : "Group",
            "NodeIdx" : 1,
            "Status" : 1,
            "Children" : [ {
              "OpName" : "Projection",
              "NodeIdx" : 0,
              "Status" : 2,
              "Children" : [ {
                "OpName" : "Filter",
                "NodeIdx" : 0,
                "Status" : 0,
                "Children" : [ {
                  "OpName" : "TableScan",
                  "NodeIdx" : 0,
                  "Status" : 1
                } ]
              } ]
            } ]
          } ]
        },
        "PrepareTimeConsumed" : 152926
      } ],
      "RootOperator" : {
        "OpName" : "Output",
        "NodeIdx" : -1,
        "Status" : 0,
        "OpStats" : {
          "CallCount" : 2,
          "TimeConsumed" : 21883
        },
        "Children" : [ {
          "OpName" : "Projection",
          "NodeIdx" : 2,
          "Status" : 3,
          "OpStats" : {
            "CallCount" : 2,
            "TimeConsumed" : 15439,
            "MemorySize" : 8,
            "InputRows" : 1,
            "InputSize" : 8,
            "OutputRows" : 1,
            "OutputSize" : 8
          },
          "Children" : [ {
            "OpName" : "Projection",
            "NodeIdx" : 1,
            "Status" : 2,
            "OpStats" : {
              "CallCount" : 2,
              "TimeConsumed" : 1094,
              "MemorySize" : 8,
              "OutputRows" : 1,
              "OutputSize" : 8
            },
            "Children" : [ {
              "OpName" : "MergeGroup",
              "NodeIdx" : 1,
              "Status" : 0,
              "OpStats" : {
                "CallCount" : 2,
                "TimeConsumed" : 15712,
                "MemorySize" : 8
              },
              "Children" : [ {
                "OpName" : "Merge",
                "NodeIdx" : 1,
                "Status" : 0,
                "OpStats" : {
                  "CallCount" : 5,
                  "TimeConsumed" : 29182,
                  "WaitTimeConsumed" : 81917495
                }
              } ]
            } ]
          } ]
        } ]
      },
      "PrepareTimeConsumed" : 2485
    } ]
  },
  "NewPlanStats" : {
    "ParseStage" : {
      "ParseDuration" : 162026,
      "ParseStartTime" : "2024-10-24T14:30:46.445648028+08:00"
    },
    "PlanStage" : {
      "PlanDuration" : 649910,
      "PlanStartTime" : "2024-10-24T14:30:46.450048925+08:00",
      "BuildPlanS3Request" : { },
      "BuildPlanStatsS3" : { },
      "BuildPlanStatsDuration" : 3457758,
      "BuildPlanResolveVarDuration" : 3130
    },
    "CompileStage" : {
      "CompileDuration" : 299370,
      "CompileStartTime" : "2024-10-24T14:30:46.450446169+08:00",
      "CompileS3Request" : { },
      "CompileExpandRangesS3" : { },
      "CompileTableScanDuration" : 33791
    },
    "PrepareRunStage" : {
      "ScopePrepareDuration" : 175835,
      "CompilePreRunOnceDuration" : 12456,
      "ScopePrepareS3Request" : { },
      "BuildReaderDuration" : 144702
    },
    "ExecuteStage" : {
      "ExecutionDuration" : 82152198,
      "ExecutionStartTime" : "2024-10-24T14:30:46.450762791+08:00",
      "ExecutionEndTime" : "2024-10-24T14:30:46.532914985+08:00",
      "OutputDuration" : 16798
    },
    "OtherStage" : {
      "TxnIncrStatementS3" : { }
    },
    "IOAccessTimeConsumption" : 214917833,
    "S3FSPrefetchFileIOMergerTimeConsumption" : 0,
    "LocalFSReadIOMergerTimeConsumption" : 1381899,
    "S3FSReadIOMergerTimeConsumption" : 0,
    "WaitActive" : 0
  }
}`

	explain_normal_res := `LOCAL SCOPES:
Scope 1 (Magic: Merge, mcpu: 1, Receiver: [0])
  Pipeline: └── Output
                └── Projection
                    └── Projection
                        └── MergeGroup
                            └── Merge
  PreScopes: {
    Scope 1 (Magic: Remote, mcpu: 4, Receiver: [])
      DataSource: cloud_device.real_time_position[time_stamp distance]
      Pipeline: └── Connector to MergeReceiver 0
                    └── Group
                        └── Projection
                            └── Filter
                                └── TableScan
      PreScopes: {
        Scope 1 (Magic: Normal, mcpu: 0, Receiver: [])
          PreScopes: {
            Scope 1 (Magic: Normal, mcpu: 1, Receiver: [])
              DataSource: cloud_device.real_time_position[time_stamp distance]
              Pipeline: └── Connector to MergeReceiver 0
                            └── Group
                                └── Projection
                                    └── Filter
                                        └── TableScan
            Scope 2 (Magic: Normal, mcpu: 1, Receiver: [])
              DataSource: cloud_device.real_time_position[time_stamp distance]
              Pipeline: └── Connector to MergeReceiver 0
                            └── Group
                                └── Projection
                                    └── Filter
                                        └── TableScan
            Scope 3 (Magic: Normal, mcpu: 1, Receiver: [])
              DataSource: cloud_device.real_time_position[time_stamp distance]
              Pipeline: └── Connector to MergeReceiver 0
                            └── Group
                                └── Projection
                                    └── Filter
                                        └── TableScan
            Scope 4 (Magic: Normal, mcpu: 1, Receiver: [])
              DataSource: cloud_device.real_time_position[time_stamp distance]
              Pipeline: └── Connector to MergeReceiver 0
                            └── Group
                                └── Projection
                                    └── Filter
                                        └── TableScan
          }
      }
  }`
	explain_verbose_res := `Overview:
	MemoryUsage:5623960B,  DiskI/O:66715122B,  NewWorkI/O:0B,  RetryTime: 0
	S3List:0, S3Head:0, S3Put:0, S3Get:0, S3Delete:0, S3DeleteMul:0, S3InputEstByRows((0+0)/8192):0.0000 
	CPU Usage: 
		- Total CPU Time: 91033157ns 
		- CPU Time Detail: Parse(162026)+BuildPlan(649910)+Compile(299370)+PhyExec(304651393)+PrepareRun(178265)-PreRunWaitLock(0)-IOAccess(214917833)-IOMerge(0)
Physical Plan Deployment:
LOCAL SCOPES:
Scope 1 (Magic: Merge, mcpu: 1, Receiver: [0])
  Pipeline: └── Output (idx:-1, isFirst:false, isLast:false)
                └── Projection (idx:2, isFirst:true, isLast:true)
                    └── Projection (idx:1, isFirst:false, isLast:true)
                        └── MergeGroup (idx:1, isFirst:false, isLast:false)
                            └── Merge (idx:1, isFirst:false, isLast:false)
  PreScopes: {
    Scope 1 (Magic: Remote, mcpu: 4, Receiver: [])
      DataSource: cloud_device.real_time_position[time_stamp distance]
      Pipeline: └── Connector (idx:1, isFirst:false, isLast:false) to MergeReceiver 0
                    └── Group (idx:1, isFirst:true, isLast:false)
                        └── Projection (idx:0, isFirst:false, isLast:true)
                            └── Filter (idx:0, isFirst:false, isLast:false)
                                └── TableScan (idx:0, isFirst:true, isLast:false)
      PreScopes: {
        Scope 1 (Magic: Normal, mcpu: 0, Receiver: [])
          PreScopes: {
            Scope 1 (Magic: Normal, mcpu: 1, Receiver: [])
              DataSource: cloud_device.real_time_position[time_stamp distance]
              Pipeline: └── Connector (idx:1, isFirst:false, isLast:false) to MergeReceiver 0
                            └── Group (idx:1, isFirst:true, isLast:false)
                                └── Projection (idx:0, isFirst:false, isLast:true)
                                    └── Filter (idx:0, isFirst:false, isLast:false)
                                        └── TableScan (idx:0, isFirst:true, isLast:false)
            Scope 2 (Magic: Normal, mcpu: 1, Receiver: [])
              DataSource: cloud_device.real_time_position[time_stamp distance]
              Pipeline: └── Connector (idx:1, isFirst:false, isLast:false) to MergeReceiver 0
                            └── Group (idx:1, isFirst:true, isLast:false)
                                └── Projection (idx:0, isFirst:false, isLast:true)
                                    └── Filter (idx:0, isFirst:false, isLast:false)
                                        └── TableScan (idx:0, isFirst:true, isLast:false)
            Scope 3 (Magic: Normal, mcpu: 1, Receiver: [])
              DataSource: cloud_device.real_time_position[time_stamp distance]
              Pipeline: └── Connector (idx:1, isFirst:false, isLast:false) to MergeReceiver 0
                            └── Group (idx:1, isFirst:true, isLast:false)
                                └── Projection (idx:0, isFirst:false, isLast:true)
                                    └── Filter (idx:0, isFirst:false, isLast:false)
                                        └── TableScan (idx:0, isFirst:true, isLast:false)
            Scope 4 (Magic: Normal, mcpu: 1, Receiver: [])
              DataSource: cloud_device.real_time_position[time_stamp distance]
              Pipeline: └── Connector (idx:1, isFirst:false, isLast:false) to MergeReceiver 0
                            └── Group (idx:1, isFirst:true, isLast:false)
                                └── Projection (idx:0, isFirst:false, isLast:true)
                                    └── Filter (idx:0, isFirst:false, isLast:false)
                                        └── TableScan (idx:0, isFirst:true, isLast:false)
          }
      }
  }`

	explain_analyze_res := `Overview:
	MemoryUsage:5623960B,  DiskI/O:66715122B,  NewWorkI/O:0B,  RetryTime: 0
	S3List:0, S3Head:0, S3Put:0, S3Get:0, S3Delete:0, S3DeleteMul:0, S3InputEstByRows((0+0)/8192):0.0000 
	CPU Usage: 
		- Total CPU Time: 91033157ns 
		- CPU Time Detail: Parse(162026)+BuildPlan(649910)+Compile(299370)+PhyExec(304651393)+PrepareRun(178265)-PreRunWaitLock(0)-IOAccess(214917833)-IOMerge(0)
	Query Build Plan Stage:
		- CPU Time: 649910ns 
		- S3List:0, S3Head:0, S3Put:0, S3Get:0, S3Delete:0, S3DeleteMul:0
		- Call Stats Duration: 3457758ns 
	Query Compile Stage:
		- CPU Time: 299370ns 
		- S3List:0, S3Head:0, S3Put:0, S3Get:0, S3Delete:0, S3DeleteMul:0
		- Compile TableScan Duration: 33791ns 
	Query Prepare Exec Stage:
		- CPU Time: 178265ns 
		- CompilePreRunOnce Duration: 12456ns 
		- PreRunOnce WaitLock: 0ns 
		- ScopePrepareTimeConsumed: 165809ns 
		- BuildReader Duration: 144702ns 
		- S3List:0, S3Head:0, S3Put:0, S3Get:0, S3Delete:0, S3DeleteMul:0
	Query Execution Stage:
		- CPU Time: 304651393ns 
		- S3List:0, S3Head:0, S3Put:0, S3Get:0, S3Delete:0, S3DeleteMul:0
		- MemoryUsage: 5623960B,  DiskI/O: 66715122B,  NewWorkI/O:0B
Physical Plan Deployment:
LOCAL SCOPES:
Scope 1 (Magic: Merge, mcpu: 1, Receiver: [0])
  Pipeline: └── Output (idx:-1, isFirst:false, isLast:false) CallNum:2 TimeCost:21883ns WaitTime:0ns InRows:0 OutRows:0 InSize:0bytes InBlock:0 OutSize:0bytes MemSize:0bytes ScanBytes:0bytes NetworkIO:0bytes DiskIO:0bytes WrittenRows:0 DeletedRows:0 S3List:0 S3Head:0 S3Put:0 S3Get:0 S3Delete:0 S3DeleteMul:0
                └── Projection (idx:2, isFirst:true, isLast:true) CallNum:2 TimeCost:15439ns WaitTime:0ns InRows:1 OutRows:1 InSize:8bytes InBlock:0 OutSize:8bytes MemSize:8bytes ScanBytes:0bytes NetworkIO:0bytes DiskIO:0bytes WrittenRows:0 DeletedRows:0 S3List:0 S3Head:0 S3Put:0 S3Get:0 S3Delete:0 S3DeleteMul:0
                    └── Projection (idx:1, isFirst:false, isLast:true) CallNum:2 TimeCost:1094ns WaitTime:0ns InRows:0 OutRows:1 InSize:0bytes InBlock:0 OutSize:8bytes MemSize:8bytes ScanBytes:0bytes NetworkIO:0bytes DiskIO:0bytes WrittenRows:0 DeletedRows:0 S3List:0 S3Head:0 S3Put:0 S3Get:0 S3Delete:0 S3DeleteMul:0
                        └── MergeGroup (idx:1, isFirst:false, isLast:false) CallNum:2 TimeCost:15712ns WaitTime:0ns InRows:0 OutRows:0 InSize:0bytes InBlock:0 OutSize:0bytes MemSize:8bytes ScanBytes:0bytes NetworkIO:0bytes DiskIO:0bytes WrittenRows:0 DeletedRows:0 S3List:0 S3Head:0 S3Put:0 S3Get:0 S3Delete:0 S3DeleteMul:0
                            └── Merge (idx:1, isFirst:false, isLast:false) CallNum:5 TimeCost:29182ns WaitTime:81917495ns InRows:0 OutRows:0 InSize:0bytes InBlock:0 OutSize:0bytes MemSize:0bytes ScanBytes:0bytes NetworkIO:0bytes DiskIO:0bytes WrittenRows:0 DeletedRows:0 S3List:0 S3Head:0 S3Put:0 S3Get:0 S3Delete:0 S3DeleteMul:0
  PreScopes: {
    Scope 1 (Magic: Remote, mcpu: 4, Receiver: [])
      DataSource: cloud_device.real_time_position[time_stamp distance]
      Pipeline: └── Connector (idx:1, isFirst:false, isLast:false) to MergeReceiver 0
                    └── Group (idx:1, isFirst:true, isLast:false)
                        └── Projection (idx:0, isFirst:false, isLast:true)
                            └── Filter (idx:0, isFirst:false, isLast:false)
                                └── TableScan (idx:0, isFirst:true, isLast:false)
      PreScopes: {
        Scope 1 (Magic: Normal, mcpu: 0, Receiver: [])
          PreScopes: {
            Scope 1 (Magic: Normal, mcpu: 1, Receiver: [])
              DataSource: cloud_device.real_time_position[time_stamp distance]
              Pipeline: └── Connector (idx:1, isFirst:false, isLast:false) CallNum:2 TimeCost:22677ns WaitTime:0ns InRows:0 OutRows:0 InSize:0bytes InBlock:0 OutSize:0bytes MemSize:0bytes ScanBytes:0bytes NetworkIO:0bytes DiskIO:0bytes WrittenRows:0 DeletedRows:0 S3List:0 S3Head:0 S3Put:0 S3Get:0 S3Delete:0 S3DeleteMul:0 to MergeReceiver 0
                            └── Group (idx:1, isFirst:true, isLast:false) CallNum:2 TimeCost:338784ns WaitTime:0ns InRows:15593 OutRows:0 InSize:124744bytes InBlock:0 OutSize:0bytes MemSize:0bytes ScanBytes:0bytes NetworkIO:0bytes DiskIO:0bytes WrittenRows:0 DeletedRows:0 S3List:0 S3Head:0 S3Put:0 S3Get:0 S3Delete:0 S3DeleteMul:0
                                └── Projection (idx:0, isFirst:false, isLast:true) CallNum:153 TimeCost:61454ns WaitTime:0ns InRows:0 OutRows:15593 InSize:0bytes InBlock:0 OutSize:124744bytes MemSize:29800bytes ScanBytes:0bytes NetworkIO:0bytes DiskIO:0bytes WrittenRows:0 DeletedRows:0 S3List:0 S3Head:0 S3Put:0 S3Get:0 S3Delete:0 S3DeleteMul:0
                                    └── Filter (idx:0, isFirst:false, isLast:false) CallNum:153 TimeCost:16747009ns WaitTime:0ns InRows:0 OutRows:0 InSize:0bytes InBlock:0 OutSize:0bytes MemSize:1245184bytes ScanBytes:0bytes NetworkIO:0bytes DiskIO:0bytes WrittenRows:0 DeletedRows:0 S3List:0 S3Head:0 S3Put:0 S3Get:0 S3Delete:0 S3DeleteMul:0
                                        └── TableScan (idx:0, isFirst:true, isLast:false) CallNum:153 TimeCost:58472333ns WaitTime:0ns InRows:1245184 OutRows:0 InSize:19922944bytes InBlock:0 OutSize:0bytes MemSize:131072bytes ScanBytes:0bytes NetworkIO:0bytes DiskIO:16491709bytes WrittenRows:0 DeletedRows:0 S3List:0 S3Head:0 S3Put:0 S3Get:0 S3Delete:0 S3DeleteMul:0
            Scope 2 (Magic: Normal, mcpu: 1, Receiver: [])
              DataSource: cloud_device.real_time_position[time_stamp distance]
              Pipeline: └── Connector (idx:1, isFirst:false, isLast:false) CallNum:2 TimeCost:9887ns WaitTime:0ns InRows:0 OutRows:0 InSize:0bytes InBlock:0 OutSize:0bytes MemSize:0bytes ScanBytes:0bytes NetworkIO:0bytes DiskIO:0bytes WrittenRows:0 DeletedRows:0 S3List:0 S3Head:0 S3Put:0 S3Get:0 S3Delete:0 S3DeleteMul:0 to MergeReceiver 0
                            └── Group (idx:1, isFirst:true, isLast:false) CallNum:2 TimeCost:319777ns WaitTime:0ns InRows:15405 OutRows:0 InSize:123240bytes InBlock:0 OutSize:0bytes MemSize:0bytes ScanBytes:0bytes NetworkIO:0bytes DiskIO:0bytes WrittenRows:0 DeletedRows:0 S3List:0 S3Head:0 S3Put:0 S3Get:0 S3Delete:0 S3DeleteMul:0
                                └── Projection (idx:0, isFirst:false, isLast:true) CallNum:154 TimeCost:66538ns WaitTime:0ns InRows:0 OutRows:15405 InSize:0bytes InBlock:0 OutSize:123240bytes MemSize:18864bytes ScanBytes:0bytes NetworkIO:0bytes DiskIO:0bytes WrittenRows:0 DeletedRows:0 S3List:0 S3Head:0 S3Put:0 S3Get:0 S3Delete:0 S3DeleteMul:0
                                    └── Filter (idx:0, isFirst:false, isLast:false) CallNum:154 TimeCost:18563940ns WaitTime:0ns InRows:0 OutRows:0 InSize:0bytes InBlock:0 OutSize:0bytes MemSize:1253376bytes ScanBytes:0bytes NetworkIO:0bytes DiskIO:0bytes WrittenRows:0 DeletedRows:0 S3List:0 S3Head:0 S3Put:0 S3Get:0 S3Delete:0 S3DeleteMul:0
                                        └── TableScan (idx:0, isFirst:true, isLast:false) CallNum:154 TimeCost:61209774ns WaitTime:0ns InRows:1253376 OutRows:0 InSize:20054016bytes InBlock:0 OutSize:0bytes MemSize:131072bytes ScanBytes:0bytes NetworkIO:0bytes DiskIO:16797879bytes WrittenRows:0 DeletedRows:0 S3List:0 S3Head:0 S3Put:0 S3Get:0 S3Delete:0 S3DeleteMul:0
            Scope 3 (Magic: Normal, mcpu: 1, Receiver: [])
              DataSource: cloud_device.real_time_position[time_stamp distance]
              Pipeline: └── Connector (idx:1, isFirst:false, isLast:false) CallNum:2 TimeCost:2950ns WaitTime:0ns InRows:0 OutRows:0 InSize:0bytes InBlock:0 OutSize:0bytes MemSize:0bytes ScanBytes:0bytes NetworkIO:0bytes DiskIO:0bytes WrittenRows:0 DeletedRows:0 S3List:0 S3Head:0 S3Put:0 S3Get:0 S3Delete:0 S3DeleteMul:0 to MergeReceiver 0
                            └── Group (idx:1, isFirst:true, isLast:false) CallNum:2 TimeCost:396141ns WaitTime:0ns InRows:19167 OutRows:0 InSize:153336bytes InBlock:0 OutSize:0bytes MemSize:0bytes ScanBytes:0bytes NetworkIO:0bytes DiskIO:0bytes WrittenRows:0 DeletedRows:0 S3List:0 S3Head:0 S3Put:0 S3Get:0 S3Delete:0 S3DeleteMul:0
                                └── Projection (idx:0, isFirst:false, isLast:true) CallNum:154 TimeCost:82273ns WaitTime:0ns InRows:0 OutRows:19167 InSize:0bytes InBlock:0 OutSize:153336bytes MemSize:24624bytes ScanBytes:0bytes NetworkIO:0bytes DiskIO:0bytes WrittenRows:0 DeletedRows:0 S3List:0 S3Head:0 S3Put:0 S3Get:0 S3Delete:0 S3DeleteMul:0
                                    └── Filter (idx:0, isFirst:false, isLast:false) CallNum:154 TimeCost:17764543ns WaitTime:0ns InRows:0 OutRows:0 InSize:0bytes InBlock:0 OutSize:0bytes MemSize:1253376bytes ScanBytes:0bytes NetworkIO:0bytes DiskIO:0bytes WrittenRows:0 DeletedRows:0 S3List:0 S3Head:0 S3Put:0 S3Get:0 S3Delete:0 S3DeleteMul:0
                                        └── TableScan (idx:0, isFirst:true, isLast:false) CallNum:154 TimeCost:58087865ns WaitTime:0ns InRows:1253376 OutRows:0 InSize:20054016bytes InBlock:0 OutSize:0bytes MemSize:131072bytes ScanBytes:0bytes NetworkIO:0bytes DiskIO:16717018bytes WrittenRows:0 DeletedRows:0 S3List:0 S3Head:0 S3Put:0 S3Get:0 S3Delete:0 S3DeleteMul:0
            Scope 4 (Magic: Normal, mcpu: 1, Receiver: [])
              DataSource: cloud_device.real_time_position[time_stamp distance]
              Pipeline: └── Connector (idx:1, isFirst:false, isLast:false) CallNum:2 TimeCost:6821ns WaitTime:0ns InRows:0 OutRows:0 InSize:0bytes InBlock:0 OutSize:0bytes MemSize:0bytes ScanBytes:0bytes NetworkIO:0bytes DiskIO:0bytes WrittenRows:0 DeletedRows:0 S3List:0 S3Head:0 S3Put:0 S3Get:0 S3Delete:0 S3DeleteMul:0 to MergeReceiver 0
                            └── Group (idx:1, isFirst:true, isLast:false) CallNum:2 TimeCost:374224ns WaitTime:0ns InRows:19366 OutRows:0 InSize:154928bytes InBlock:0 OutSize:0bytes MemSize:0bytes ScanBytes:0bytes NetworkIO:0bytes DiskIO:0bytes WrittenRows:0 DeletedRows:0 S3List:0 S3Head:0 S3Put:0 S3Get:0 S3Delete:0 S3DeleteMul:0
                                └── Projection (idx:0, isFirst:false, isLast:true) CallNum:154 TimeCost:59683ns WaitTime:0ns InRows:0 OutRows:19366 InSize:0bytes InBlock:0 OutSize:154928bytes MemSize:26360bytes ScanBytes:0bytes NetworkIO:0bytes DiskIO:0bytes WrittenRows:0 DeletedRows:0 S3List:0 S3Head:0 S3Put:0 S3Get:0 S3Delete:0 S3DeleteMul:0
                                    └── Filter (idx:0, isFirst:false, isLast:false) CallNum:154 TimeCost:17421140ns WaitTime:0ns InRows:0 OutRows:0 InSize:0bytes InBlock:0 OutSize:0bytes MemSize:1248064bytes ScanBytes:0bytes NetworkIO:0bytes DiskIO:0bytes WrittenRows:0 DeletedRows:0 S3List:0 S3Head:0 S3Put:0 S3Get:0 S3Delete:0 S3DeleteMul:0
                                        └── TableScan (idx:0, isFirst:true, isLast:false) CallNum:154 TimeCost:54582153ns WaitTime:0ns InRows:1248064 OutRows:0 InSize:19969024bytes InBlock:0 OutSize:0bytes MemSize:131072bytes ScanBytes:0bytes NetworkIO:0bytes DiskIO:16708516bytes WrittenRows:0 DeletedRows:0 S3List:0 S3Head:0 S3Put:0 S3Get:0 S3Delete:0 S3DeleteMul:0
          }
      }
  }`

	fmt.Printf("%d, %d, %d", len(explain_normal_res), len(explain_verbose_res), len(explain_analyze_res))
	return []tcTemp{
		{
			info: "test mo_explain_phy normal",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{exec_plan_json},
					[]bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"normal"},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{explain_normal_res},
				[]bool{false}),
		},
		{
			info: "test mo_explain_phy verbose",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{exec_plan_json},
					[]bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"verbose"},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{explain_verbose_res},
				[]bool{false}),
		},
		{
			info: "test mo_explain_phy analyze",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{exec_plan_json},
					[]bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"analyze"},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{explain_analyze_res},
				[]bool{false}),
		},
	}
}

func Test_buildInM0ExplainPhy(t *testing.T) {
	testCases := initMoExplainPhyTestCase()

	// do the test work.
	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, buildInM0ExplainPhy)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}
