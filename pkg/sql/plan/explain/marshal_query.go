// Copyright 2021 - 2022 Matrix Origin
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

package explain

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/sql/models"
	"github.com/matrixorigin/matrixone/pkg/sql/util"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic"
)

var errUnsupportedNodeType = "Unsupported node type when plan is serialized to json"

// The global variable is used to serialize plan and avoid objects being repeatedly created
var MarshalPlanOptions = ExplainOptions{
	Verbose: true,
	Analyze: true,
	Format:  EXPLAIN_FORMAT_TEXT,
}

func ConvertNode(ctx context.Context, node *plan.Node, options *ExplainOptions) (*models.Node, error) {
	marshalNodeImpl := NewMarshalNodeImpl(node)
	newNode := &models.Node{
		NodeId:     strconv.FormatInt(int64(node.NodeId), 10),
		Statistics: marshalNodeImpl.GetStatistics(ctx, options),
		Stats:      marshalNodeImpl.GetStats(),
		TotalStats: marshalNodeImpl.GetTotalStats(),
	}
	name, err := marshalNodeImpl.GetNodeName(ctx)
	if err != nil {
		return nil, err
	}
	newNode.Name = name

	title, err := marshalNodeImpl.GetNodeTitle(ctx, options)
	if err != nil {
		return nil, err
	}
	newNode.Title = title

	labels, err := marshalNodeImpl.GetNodeLabels(ctx, options)
	if err != nil {
		return nil, err
	}
	newNode.Labels = labels
	return newNode, nil
}

type MarshalNode interface {
	GetNodeName(ctx context.Context) (string, error)
	GetNodeTitle(ctx context.Context, options *ExplainOptions) (string, error)
	GetNodeLabels(ctx context.Context, options *ExplainOptions) ([]models.Label, error)
	GetStatistics(ctx context.Context, options *ExplainOptions) models.Statistics
	GetStats() models.Stats
	GetTotalStats() models.TotalStats
}

type MarshalNodeImpl struct {
	node *plan.Node
}

func NewMarshalNodeImpl(node *plan.Node) *MarshalNodeImpl {
	return &MarshalNodeImpl{
		node: node,
	}
}

func (m MarshalNodeImpl) GetStats() models.Stats {
	if m.node.Stats != nil {
		var hashmapSize float64
		if m.node.Stats.HashmapStats != nil {
			hashmapSize = m.node.Stats.HashmapStats.HashmapSize
		}
		return models.Stats{
			BlockNum:    m.node.Stats.BlockNum,
			Cost:        m.node.Stats.Cost,
			Outcnt:      m.node.Stats.Outcnt,
			HashmapSize: hashmapSize,
			Rowsize:     m.node.Stats.Rowsize,
		}
	} else {
		return models.Stats{}
	}
}

func (m MarshalNodeImpl) GetNodeName(ctx context.Context) (string, error) {
	// Get the Node Name
	if value, ok := nodeTypeToNameMap[m.node.NodeType]; ok {
		return value, nil
	} else {
		return "", moerr.NewInternalError(ctx, errUnsupportedNodeType)
	}
}

func (m MarshalNodeImpl) GetNodeTitle(ctx context.Context, options *ExplainOptions) (string, error) {
	//var result string
	buf := bytes.NewBuffer(make([]byte, 0, 400))
	var err error
	switch m.node.NodeType {
	case plan.Node_TABLE_SCAN, plan.Node_EXTERNAL_SCAN, plan.Node_MATERIAL_SCAN, plan.Node_SOURCE_SCAN:
		//"title" : "SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.DATE_DIM",
		if m.node.ObjRef != nil {
			buf.WriteString(m.node.ObjRef.GetSchemaName() + "." + m.node.ObjRef.GetObjName())
		} else if m.node.TableDef != nil {
			buf.WriteString(m.node.TableDef.GetName())
		} else {
			return "", moerr.NewInvalidInput(ctx, "Table definition not found when plan is serialized to json")
		}
	case plan.Node_DELETE:
		if m.node.DeleteCtx != nil {
			ctx := m.node.DeleteCtx.Ref
			buf.WriteString(ctx.SchemaName + "." + ctx.ObjName)
		} else {
			return "", moerr.NewInternalError(ctx, "Table definition not found when plan is serialized to json")
		}
	case plan.Node_INSERT:
		if m.node.InsertCtx != nil {
			ctx := m.node.InsertCtx.Ref
			buf.WriteString(ctx.SchemaName + "." + ctx.ObjName)
		} else {
			return "", moerr.NewInternalError(ctx, "Table definition not found when plan is serialized to json")
		}
	case plan.Node_PROJECT, plan.Node_VALUE_SCAN, plan.Node_UNION, plan.Node_UNION_ALL,
		plan.Node_INTERSECT, plan.Node_INTERSECT_ALL, plan.Node_MINUS:
		//"title" : "STORE.S_STORE_NAME,STORE.S_STORE_ID,WSS.D_WEEK_SEQ"
		exprs := NewExprListDescribeImpl(m.node.ProjectList)
		err = exprs.GetDescription(ctx, options, buf)
		if err != nil {
			return "", err
		}
	case plan.Node_AGG:
		// "SUM(IFF(DATE_DIM.D_DAY_NAME = 'Sunday', STORE_SALES.SS_SALES_PRICE, null))"
		exprs := NewExprListDescribeImpl(m.node.AggList)
		err = exprs.GetDescription(ctx, options, buf)
		if err != nil {
			return "", err
		}
	case plan.Node_FILTER:
		//"title" : "(D_0.D_MONTH_SEQ >= 1189) AND (D_0.D_MONTH_SEQ <= 1200)",
		exprs := NewExprListDescribeImpl(m.node.FilterList)
		err = exprs.GetDescription(ctx, options, buf)
		if err != nil {
			return "", err
		}
	case plan.Node_JOIN:
		//"title" : "(DATE_DIM.D_DATE_SK = STORE_SALES.SS_SOLD_DATE_SK)",
		exprs := NewExprListDescribeImpl(m.node.OnList)
		err = exprs.GetDescription(ctx, options, buf)
		if err != nil {
			return "", err
		}
	case plan.Node_SORT:
		//"title" : "STORE.S_STORE_NAME ASC NULLS LAST,STORE.S_STORE_ID ASC NULLS LAST,WSS.D_WEEK_SEQ ASC NULLS LAST",
		orderByDescImpl := NewOrderByDescribeImpl(m.node.OrderBy)
		err = orderByDescImpl.GetDescription(ctx, options, buf)
		if err != nil {
			return "", err
		}
	case plan.Node_PRE_INSERT:
		return "preinsert", nil
	case plan.Node_PRE_INSERT_UK:
		return "preinsert_uk", nil
	case plan.Node_PRE_INSERT_SK:
		return "preinsert_sk", nil
	case plan.Node_PRE_DELETE:
		return "predelete", nil
	case plan.Node_SINK:
		return "sink", nil
	case plan.Node_SINK_SCAN:
		return "sink_scan", nil
	case plan.Node_RECURSIVE_SCAN:
		return "recursive_scan", nil
	case plan.Node_RECURSIVE_CTE:
		return "cte_scan", nil
	case plan.Node_ON_DUPLICATE_KEY:
		return "on_duplicate_key", nil
	case plan.Node_LOCK_OP:
		return "lock_op", nil
	case plan.Node_ASSERT:
		return "assert", nil
	case plan.Node_BROADCAST:
		return "broadcast", nil
	case plan.Node_SPLIT:
		return "split", nil
	case plan.Node_GATHER:
		return "gather", nil
	case plan.Node_REPLACE:
		return "replace", nil
	case plan.Node_TIME_WINDOW:
		return "time_window", nil
	case plan.Node_FILL:
		return "fill", nil
	case plan.Node_PARTITION:
		return "partition", nil
	case plan.Node_FUNCTION_SCAN:
		//"title" : "SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.DATE_DIM",
		if m.node.TableDef != nil && m.node.TableDef.TblFunc != nil {
			fmt.Fprintf(buf, "Table Function[%s]", m.node.TableDef.TblFunc.Name)
		} else {
			return "", moerr.NewInvalidInput(ctx, "Table definition not found when plan is serialized to json")
		}
	case plan.Node_FUZZY_FILTER:
		return "fuzzy_filter", nil
	case plan.Node_SAMPLE:
		return "sample", nil
	case plan.Node_UNKNOWN:
		return "unknown", nil
	case plan.Node_DISTINCT:
		return "distinct", nil
	case plan.Node_UNIQUE:
		return "unique", nil
	case plan.Node_MINUS_ALL:
		return "minus_all", nil
	case plan.Node_EXTERNAL_FUNCTION:
		return "external_function", nil
	case plan.Node_WINDOW:
		return "window", nil
	case plan.Node_MATERIAL:
		return "mterial", nil
	case plan.Node_APPLY:
		return "apply", nil
	case plan.Node_MULTI_UPDATE:
		return "multi_update", nil
	default:
		return "", moerr.NewInternalError(ctx, errUnsupportedNodeType)
	}
	return strings.TrimSpace(buf.String()), nil
}

func (m MarshalNodeImpl) GetNodeLabels(ctx context.Context, options *ExplainOptions) ([]models.Label, error) {
	labels := make([]models.Label, 0)

	// 1. Handling unique label information for different nodes
	switch m.node.NodeType {
	case plan.Node_TABLE_SCAN, plan.Node_EXTERNAL_SCAN, plan.Node_MATERIAL_SCAN, plan.Node_SOURCE_SCAN:
		tableDef := m.node.TableDef
		objRef := m.node.ObjRef
		fullTableName := ""
		if objRef != nil {
			fullTableName += objRef.GetSchemaName() + "." + objRef.GetObjName()
		} else if tableDef != nil {
			fullTableName += tableDef.GetName()
		} else {
			return nil, moerr.NewInternalError(ctx, "Table definition not found when plan is serialized to json")
		}

		labels = append(labels, models.Label{
			Name:  Label_Table_Name, //"Full table name",
			Value: fullTableName,
		})

		// "name" : "Columns (2 / 28)",
		columns := GetTableColsLableValue(ctx, tableDef.Cols, options)

		labels = append(labels, models.Label{
			Name:  Label_Table_Columns, //"Columns",
			Value: columns,
		})

		labels = append(labels, models.Label{
			Name:  Label_Total_Columns, //"Total columns",
			Value: len(tableDef.Name2ColIndex),
		})

		labels = append(labels, models.Label{
			Name:  Label_Scan_Columns, //"Scan columns",
			Value: len(tableDef.Cols),
		})

		if len(m.node.BlockFilterList) > 0 {
			value, err := GetExprsLabelValue(ctx, m.node.BlockFilterList, options)
			if err != nil {
				return nil, err
			}
			labels = append(labels, models.Label{
				Name:  Label_Block_Filter_Conditions, // "Block Filter conditions",
				Value: value,
			})
		}
	case plan.Node_FUNCTION_SCAN:
		tableDef := m.node.TableDef
		fullTableName := ""
		if tableDef != nil && tableDef.TblFunc != nil {
			fullTableName += tableDef.TblFunc.GetName()
		} else {
			return nil, moerr.NewInternalError(ctx, "Table Function definition not found when plan is serialized to json")
		}

		labels = append(labels, models.Label{
			Name:  Label_Table_Name, //"Full table name",
			Value: fullTableName,
		})

		// "name" : "Columns (2 / 28)",
		columns := GetTableColsLableValue(ctx, tableDef.Cols, options)

		labels = append(labels, models.Label{
			Name:  Label_Table_Columns, //"Columns",
			Value: columns,
		})

		labels = append(labels, models.Label{
			Name:  Label_Total_Columns, //"Total columns",
			Value: len(tableDef.Name2ColIndex),
		})

		labels = append(labels, models.Label{
			Name:  Label_Scan_Columns, //"Scan columns",
			Value: len(tableDef.Cols),
		})

		if len(m.node.BlockFilterList) > 0 {
			value, err := GetExprsLabelValue(ctx, m.node.BlockFilterList, options)
			if err != nil {
				return nil, err
			}
			labels = append(labels, models.Label{
				Name:  Label_Block_Filter_Conditions, // "Block Filter conditions",
				Value: value,
			})
		}
	case plan.Node_INSERT:
		objRef := m.node.InsertCtx.Ref
		fullTableName := ""
		if objRef != nil {
			fullTableName += objRef.GetSchemaName() + "." + objRef.GetObjName()
		} else {
			return nil, moerr.NewInternalError(ctx, "Table definition not found when plan is serialized to json")
		}

		labels = append(labels, models.Label{
			Name:  Label_Table_Name, //"Full table name",
			Value: fullTableName,
		})
	case plan.Node_DELETE:
		if m.node.DeleteCtx != nil {
			deleteTableNames := GetDeleteTableLabelValue(m.node.DeleteCtx)
			labels = append(labels, models.Label{
				Name:  Label_Table_Name, //"Full table name",
				Value: deleteTableNames,
			})
		} else {
			return nil, moerr.NewInvalidInput(ctx, "Table definition not found when plan is serialized to json")
		}
	case plan.Node_PROJECT:
		value, err := GetExprsLabelValue(ctx, m.node.ProjectList, options)
		if err != nil {
			return nil, err
		}
		labels = append(labels, models.Label{
			Name:  Label_List_Expression, //"List of expressions",
			Value: value,
		})
	case plan.Node_AGG:
		// Get Group key info
		if len(m.node.GroupBy) > 0 {
			// Get Grouping Key
			value, err := GetExprsLabelValue(ctx, m.node.GroupBy, options)
			if err != nil {
				return nil, err
			}
			labels = append(labels, models.Label{
				Name:  Label_Grouping_Keys, //"Grouping keys",
				Value: value,
			})
		}

		// Get Aggregate function info
		if len(m.node.AggList) > 0 {
			value, err := GetExprsLabelValue(ctx, m.node.AggList, options)
			if err != nil {
				return nil, err
			}
			labels = append(labels, models.Label{
				Name:  Label_Agg_Functions, //"Aggregate functions",
				Value: value,
			})
		}
	case plan.Node_FILTER:
		value, err := GetExprsLabelValue(ctx, m.node.FilterList, options)
		if err != nil {
			return nil, err
		}
		labels = append(labels, models.Label{
			Name:  Label_Filter_Conditions, //"Filter conditions",
			Value: value,
		})
	case plan.Node_JOIN:
		// Get Join type
		labels = append(labels, models.Label{
			Name:  Label_Join_Type, //"Join type",
			Value: m.node.JoinType.String(),
		})

		// Get Join Condition info
		if len(m.node.OnList) > 0 {
			value, err := GetExprsLabelValue(ctx, m.node.OnList, options)
			if err != nil {
				return nil, err
			}
			labels = append(labels, models.Label{
				Name:  Label_Join_Conditions, //"Join conditions",
				Value: value,
			})
		}
		labels = append(labels, models.Label{
			Name:  Label_Left_NodeId, //"Left node id",
			Value: m.node.Children[0],
		})
		labels = append(labels, models.Label{
			Name:  Label_Right_NodeId, //"Right node id",
			Value: m.node.Children[1],
		})
	case plan.Node_SORT:
		result, err := GetOrderByLabelValue(ctx, m.node.OrderBy, options)
		if err != nil {
			return nil, err
		}
		labels = append(labels, models.Label{
			Name:  Label_Sort_Keys, //"Sort keys",
			Value: result,
		})
	case plan.Node_VALUE_SCAN:
		value, err := GetExprsLabelValue(ctx, m.node.ProjectList, options)
		if err != nil {
			return nil, err
		}
		labels = append(labels, models.Label{
			Name:  Label_List_Values, //"List of values",
			Value: value,
		})

		if len(m.node.BlockFilterList) > 0 {
			value, err = GetExprsLabelValue(ctx, m.node.BlockFilterList, options)
			if err != nil {
				return nil, err
			}
			labels = append(labels, models.Label{
				Name:  Label_Block_Filter_Conditions, // "Block Filter conditions",
				Value: value,
			})
		}
	case plan.Node_UNION:
		value, err := GetExprsLabelValue(ctx, m.node.ProjectList, options)
		if err != nil {
			return nil, err
		}
		labels = append(labels, models.Label{
			Name:  Label_Union_Expressions, //"Union expressions",
			Value: value,
		})
	case plan.Node_UNION_ALL:
		value, err := GetExprsLabelValue(ctx, m.node.ProjectList, options)
		if err != nil {
			return nil, err
		}
		labels = append(labels, models.Label{
			Name:  Label_Union_All_Expressions, // "Union all expressions",
			Value: value,
		})
	case plan.Node_INTERSECT:
		value, err := GetExprsLabelValue(ctx, m.node.ProjectList, options)
		if err != nil {
			return nil, err
		}
		labels = append(labels, models.Label{
			Name:  Label_Intersect_Expressions, //"Intersect expressions",
			Value: value,
		})
	case plan.Node_INTERSECT_ALL:
		value, err := GetExprsLabelValue(ctx, m.node.ProjectList, options)
		if err != nil {
			return nil, err
		}
		labels = append(labels, models.Label{
			Name:  Label_Intersect_All_Expressions, //"Intersect All expressions",
			Value: value,
		})
	case plan.Node_MINUS:
		value, err := GetExprsLabelValue(ctx, m.node.ProjectList, options)
		if err != nil {
			return nil, err
		}
		labels = append(labels, models.Label{
			Name:  Label_Minus_Expressions, //"Minus expressions",
			Value: value,
		})
	case plan.Node_PRE_INSERT:
		labels = append(labels, models.Label{
			Name:  Label_Pre_Insert, //"pre insert",
			Value: []string{},
		})
	case plan.Node_PRE_INSERT_UK:
		labels = append(labels, models.Label{
			Name:  Label_Pre_InsertUk, //"pre insert uk",
			Value: []string{},
		})
	case plan.Node_PRE_INSERT_SK:
		labels = append(labels, models.Label{
			Name:  Label_Pre_InsertSk, //"pre insert sk",
			Value: []string{},
		})
	case plan.Node_PRE_DELETE:
		labels = append(labels, models.Label{
			Name:  Label_Pre_Delete, //"pre delete",
			Value: []string{},
		})
	case plan.Node_SINK:
		labels = append(labels, models.Label{
			Name:  Label_Sink, //"sink",
			Value: []string{},
		})
	case plan.Node_SINK_SCAN:
		labels = append(labels, models.Label{
			Name:  Label_Sink_Scan, //"sink scan",
			Value: []string{},
		})
	case plan.Node_RECURSIVE_SCAN:
		labels = append(labels, models.Label{
			Name:  Label_Recursive_SCAN, //"sink scan",
			Value: []string{},
		})
	case plan.Node_RECURSIVE_CTE:
		labels = append(labels, models.Label{
			Name:  Label_Recursive_SCAN, //"sink scan",
			Value: []string{},
		})
	case plan.Node_LOCK_OP:
		labels = append(labels, models.Label{
			Name:  Label_Lock_Op, //"lock op",
			Value: []string{},
		})
	case plan.Node_TIME_WINDOW:
		labels = append(labels, models.Label{
			Name:  Label_Time_Window,
			Value: []string{},
		})
	case plan.Node_PARTITION:
		labels = append(labels, models.Label{
			Name:  Label_Partition,
			Value: []string{},
		})
	case plan.Node_BROADCAST:
		labels = append(labels, models.Label{
			Name:  Label_Boardcast,
			Value: []string{},
		})
	case plan.Node_SPLIT:
		labels = append(labels, models.Label{
			Name:  Label_Split,
			Value: []string{},
		})
	case plan.Node_GATHER:
		labels = append(labels, models.Label{
			Name:  Label_Gather,
			Value: []string{},
		})
	case plan.Node_ASSERT:
		labels = append(labels, models.Label{
			Name:  Label_Assert,
			Value: []string{},
		})
	case plan.Node_ON_DUPLICATE_KEY:
		labels = append(labels, models.Label{
			Name:  Label_On_Duplicate_Key,
			Value: []string{},
		})
	case plan.Node_FUZZY_FILTER:
		labels = append(labels, models.Label{
			Name:  Label_Fuzzy_Filter,
			Value: []string{},
		})
	case plan.Node_EXTERNAL_FUNCTION:
		labels = append(labels, models.Label{
			Name:  Label_External_Function,
			Value: []string{},
		})
	case plan.Node_FILL:
		labels = append(labels, models.Label{
			Name:  Label_Fill,
			Value: []string{},
		})
	case plan.Node_DISTINCT:
		labels = append(labels, models.Label{
			Name:  Label_Distinct,
			Value: []string{},
		})
	case plan.Node_SAMPLE:
		labels = append(labels, models.Label{
			Name:  Label_Sample,
			Value: []string{},
		})
	case plan.Node_WINDOW:
		labels = append(labels, models.Label{
			Name:  Label_Window,
			Value: []string{},
		})
	case plan.Node_MINUS_ALL:
		labels = append(labels, models.Label{
			Name:  Label_Minus_All,
			Value: []string{},
		})
	case plan.Node_UNIQUE:
		labels = append(labels, models.Label{
			Name:  Label_Unique,
			Value: []string{},
		})
	case plan.Node_REPLACE:
		labels = append(labels, models.Label{
			Name:  Label_Replace,
			Value: []string{},
		})
	case plan.Node_UNKNOWN:
		labels = append(labels, models.Label{
			Name:  Label_Unknown,
			Value: []string{},
		})
	case plan.Node_MATERIAL:
		labels = append(labels, models.Label{
			Name:  Label_Meterial,
			Value: []string{},
		})
	case plan.Node_APPLY:
		labels = append(labels, models.Label{
			Name:  Label_Apply,
			Value: []string{},
		})
	case plan.Node_MULTI_UPDATE:
		labels = append(labels, models.Label{
			Name:  Label_Apply,
			Value: []string{},
		})
	default:
		return nil, moerr.NewInternalError(ctx, errUnsupportedNodeType)
	}

	// 2. handle shared label information for all nodes, such as filter conditions
	if len(m.node.FilterList) > 0 && m.node.NodeType != plan.Node_FILTER {
		value, err := GetExprsLabelValue(ctx, m.node.FilterList, options)
		if err != nil {
			return nil, err
		}
		labels = append(labels, models.Label{
			Name:  Label_Filter_Conditions, // "Filter conditions",
			Value: value,
		})
	}

	// 3. handle `Limit` and `Offset` label information for all nodes
	if m.node.Limit != nil {
		buf := bytes.NewBuffer(make([]byte, 0, 80))
		err := describeExpr(ctx, m.node.Limit, options, buf)
		if err != nil {
			return nil, err
		}
		labels = append(labels, models.Label{
			Name:  Label_Row_Number, //"Number of rows",
			Value: buf.String(),
		})

		if m.node.Offset != nil {
			buf.Reset()
			err := describeExpr(ctx, m.node.Offset, options, buf)
			if err != nil {
				return nil, err
			}
			labels = append(labels, models.Label{
				Name:  Label_Offset, // "Offset",
				Value: buf.String(),
			})
		} else {
			labels = append(labels, models.Label{
				Name:  Label_Offset, // "Offset",
				Value: 0,
			})
		}
	}
	return labels, nil
}

const TimeConsumed = "Time Consumed"
const WaitTime = "Wait Time"
const ScanTime = "Scan Time"
const InsertTime = "Insert Time"

const InputRows = "Input Rows"
const OutputRows = "Output Rows"
const InputSize = "Input Size"
const OutputSize = "Output Size"
const MemorySize = "Memory Size"
const DiskIO = "Disk IO"
const ScanBytes = "Scan Bytes"
const S3IOInputCount = "S3 IO Input Count"
const S3IOOutputCount = "S3 IO Output Count"
const Network = "Network"

func GetStatistic4Trace(ctx context.Context, node *plan.Node, options *ExplainOptions) (s statistic.StatsArray) {
	s.Reset()
	if options.Analyze && node.AnalyzeInfo != nil {
		analyzeInfo := node.AnalyzeInfo
		s.WithTimeConsumed(float64(analyzeInfo.TimeConsumed)).
			WithMemorySize(float64(analyzeInfo.MemorySize)).
			WithS3IOInputCount(float64(analyzeInfo.S3IOInputCount)).
			WithS3IOOutputCount(float64(analyzeInfo.S3IOOutputCount))
	}
	return
}

// GetInputRowsAndInputSize return plan.Node AnalyzeInfo InputRows and InputSize.
// The method only records the original table's input data, and does not record index table's input data
// migrate ExplainData.StatisticsRead to here
func GetInputRowsAndInputSize(ctx context.Context, node *plan.Node, options *ExplainOptions) (rows int64, size int64) {
	if util.IsIndexTableName(node.TableDef.Name) {
		return 0, 0
	}

	if options.Analyze && node.AnalyzeInfo != nil {
		return node.AnalyzeInfo.InputRows, node.AnalyzeInfo.InputSize
	}
	return
}

func (m MarshalNodeImpl) GetStatistics(ctx context.Context, options *ExplainOptions) models.Statistics {
	statistics := models.NewStatistics()
	if options.Analyze && m.node.AnalyzeInfo != nil {
		analyzeInfo := m.node.AnalyzeInfo
		times := []models.StatisticValue{
			{
				Name:  TimeConsumed,
				Value: analyzeInfo.TimeConsumed,
				Unit:  Statistic_Unit_ns,
			},
			{
				Name:  WaitTime,
				Value: analyzeInfo.WaitTimeConsumed,
				Unit:  Statistic_Unit_ns,
			},
			{
				Name:  ScanTime,
				Value: analyzeInfo.ScanTime,
				Unit:  Statistic_Unit_ns,
			},
			{
				Name:  InsertTime,
				Value: analyzeInfo.InsertTime,
				Unit:  Statistic_Unit_ns,
			},
		}
		mbps := []models.StatisticValue{
			{
				Name:  InputRows,
				Value: analyzeInfo.InputRows,
				Unit:  Statistic_Unit_count, //"count",
			},
			{
				Name:  OutputRows,
				Value: analyzeInfo.OutputRows,
				Unit:  Statistic_Unit_count, //"count",
			},
			{
				Name:  InputSize,
				Value: analyzeInfo.InputSize,
				Unit:  Statistic_Unit_byte, //"byte",
			},
			{
				Name:  OutputSize,
				Value: analyzeInfo.OutputSize,
				Unit:  Statistic_Unit_byte, //"byte",
			},
		}

		mems := []models.StatisticValue{
			{
				Name:  MemorySize,
				Value: analyzeInfo.MemorySize,
				Unit:  Statistic_Unit_byte, //"byte",
			},
		}

		io := []models.StatisticValue{
			{
				Name:  DiskIO,
				Value: analyzeInfo.DiskIO,
				Unit:  Statistic_Unit_byte, //"byte",
			},
			{
				Name:  ScanBytes,
				Value: analyzeInfo.ScanBytes,
				Unit:  Statistic_Unit_byte, //"byte",
			},
			{
				Name:  S3IOInputCount,
				Value: analyzeInfo.S3IOInputCount,
				Unit:  Statistic_Unit_count, //"count",
			},
			{
				Name:  S3IOOutputCount,
				Value: analyzeInfo.S3IOOutputCount,
				Unit:  Statistic_Unit_count, //"count",
			},
		}

		nw := []models.StatisticValue{
			{
				Name:  Network,
				Value: analyzeInfo.NetworkIO,
				Unit:  Statistic_Unit_byte, //"byte",
			},
		}

		statistics.Time = append(statistics.Time, times...)
		statistics.Throughput = append(statistics.Throughput, mbps...)
		statistics.Memory = append(statistics.Memory, mems...)
		statistics.IO = append(statistics.IO, io...)
		statistics.Network = append(statistics.Network, nw...)
	}
	return *statistics
}

func (m MarshalNodeImpl) GetTotalStats() models.TotalStats {
	totalStats := models.TotalStats{
		Name: "Time spent",
		Unit: "ns",
	}
	if m.node.AnalyzeInfo != nil {
		totalStats.Value = m.node.AnalyzeInfo.TimeConsumed
	} else {
		totalStats.Value = 0
	}
	return totalStats
}

var _ MarshalNode = MarshalNodeImpl{}

func GetExprsLabelValue(ctx context.Context, exprList []*plan.Expr, options *ExplainOptions) ([]string, error) {
	if exprList == nil {
		return make([]string, 0), nil
	}
	result := make([]string, 0, len(exprList))
	buf := bytes.NewBuffer(make([]byte, 0, 200))
	for _, v := range exprList {
		buf.Reset()
		err := describeExpr(ctx, v, options, buf)
		if err != nil {
			return result, err
		}
		result = append(result, buf.String())
	}
	return result, nil
}

func GetOrderByLabelValue(ctx context.Context, orderbyList []*plan.OrderBySpec, options *ExplainOptions) ([]string, error) {
	if orderbyList == nil {
		return make([]string, 0), nil
	}
	result := make([]string, 0, len(orderbyList))
	buf := bytes.NewBuffer(make([]byte, 0, 200))
	for _, v := range orderbyList {
		buf.Reset()
		err := describeExpr(ctx, v.Expr, options, buf)
		if err != nil {
			return result, err
		}

		flagKey := int32(v.Flag)
		orderbyFlag := plan.OrderBySpec_OrderByFlag_name[flagKey]
		result = append(result, buf.String()+" "+orderbyFlag)
	}
	return result, nil
}

func GetDeleteTableLabelValue(deleteCtx *plan.DeleteCtx) []string {
	if deleteCtx == nil {
		return make([]string, 0)
	}
	result := make([]string, 0)
	ref := deleteCtx.Ref
	result = append(result, ref.SchemaName+"."+ref.ObjName)
	return result
}

func GetTableColsLableValue(ctx context.Context, cols []*plan.ColDef, options *ExplainOptions) []string {
	columns := make([]string, len(cols))
	for i, col := range cols {
		columns[i] = col.Name
	}
	return columns
}
