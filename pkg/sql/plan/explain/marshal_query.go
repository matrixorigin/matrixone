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
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// The global variable is used to serialize plan and avoid objects being repeatedly created
var MarshalPlanOptions = ExplainOptions{
	Verbose: true,
	Analyze: true,
	Format:  EXPLAIN_FORMAT_TEXT,
}

func ConvertNode(ctx context.Context, node *plan.Node, options *ExplainOptions) (*Node, error) {
	marshalNodeImpl := NewMarshalNodeImpl(node)
	newNode := &Node{
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
	GetNodeLabels(ctx context.Context, options *ExplainOptions) ([]Label, error)
	GetStatistics(ctx context.Context, options *ExplainOptions) Statistics
	GetStats() Stats
	GetTotalStats() TotalStats
}

type MarshalNodeImpl struct {
	node *plan.Node
}

func NewMarshalNodeImpl(node *plan.Node) *MarshalNodeImpl {
	return &MarshalNodeImpl{
		node: node,
	}
}

func (m MarshalNodeImpl) GetStats() Stats {
	if m.node.Stats != nil {
		return Stats{
			BlockNum:    m.node.Stats.BlockNum,
			Cost:        m.node.Stats.Cost,
			Outcnt:      m.node.Stats.Outcnt,
			HashmapSize: m.node.Stats.HashmapSize,
			Rowsize:     m.node.Stats.Rowsize,
		}
	} else {
		return Stats{}
	}
}

func (m MarshalNodeImpl) GetNodeName(ctx context.Context) (string, error) {
	var name string
	// Get the Node Name
	switch m.node.NodeType {
	case plan.Node_UNKNOWN:
		name = "UnKnow Node"
	case plan.Node_VALUE_SCAN:
		name = "Values Scan"
	case plan.Node_TABLE_SCAN:
		name = "Table Scan"
	case plan.Node_FUNCTION_SCAN:
		name = "Function Scan"
	case plan.Node_EXTERNAL_SCAN:
		name = "External Scan"
	case plan.Node_MATERIAL_SCAN:
		name = "Material Scan"
	case plan.Node_PROJECT:
		name = "Project"
	case plan.Node_EXTERNAL_FUNCTION:
		name = "External Function"
	case plan.Node_MATERIAL:
		name = "Material"
	case plan.Node_RECURSIVE_CTE:
		name = "Recursive CTE"
	case plan.Node_SINK:
		name = "Sink"
	case plan.Node_SINK_SCAN:
		name = "Sink Scan"
	case plan.Node_AGG:
		name = "Aggregate"
	case plan.Node_DISTINCT:
		name = "Distinct"
	case plan.Node_FILTER:
		name = "Filter"
	case plan.Node_JOIN:
		name = "Join"
	case plan.Node_SAMPLE:
		name = "Sample"
	case plan.Node_SORT:
		name = "Sort"
	case plan.Node_UNION:
		name = "Union"
	case plan.Node_UNION_ALL:
		name = "Union All"
	case plan.Node_UNIQUE:
		name = "Unique"
	case plan.Node_WINDOW:
		name = "Window"
	case plan.Node_BROADCAST:
		name = "Broadcast"
	case plan.Node_SPLIT:
		name = "Split"
	case plan.Node_GATHER:
		name = "Gather"
	case plan.Node_ASSERT:
		name = "Assert"
	case plan.Node_INSERT:
		name = "Insert"
	case plan.Node_UPDATE:
		name = "Update"
	case plan.Node_DELETE:
		name = "Delete"
	case plan.Node_INTERSECT:
		name = "Intersect"
	case plan.Node_INTERSECT_ALL:
		name = "Intersect All"
	case plan.Node_MINUS:
		name = "Minus"
	case plan.Node_MINUS_ALL:
		name = "Minus All"
	default:
		return name, moerr.NewInternalError(ctx, "Unsupported node type when plan is serialized to json")
	}
	return name, nil
}

func (m MarshalNodeImpl) GetNodeTitle(ctx context.Context, options *ExplainOptions) (string, error) {
	//var result string
	buf := bytes.NewBuffer(make([]byte, 0, 400))
	var err error
	switch m.node.NodeType {
	case plan.Node_TABLE_SCAN, plan.Node_EXTERNAL_SCAN, plan.Node_MATERIAL_SCAN, plan.Node_INSERT:
		//"title" : "SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.DATE_DIM",
		if m.node.ObjRef != nil {
			buf.WriteString(m.node.ObjRef.GetSchemaName() + "." + m.node.ObjRef.GetObjName())
		} else if m.node.TableDef != nil {
			buf.WriteString(m.node.TableDef.GetName())
		} else {
			return "", moerr.NewInvalidInput(ctx, "Table definition not found when plan is serialized to json")
		}
	case plan.Node_UPDATE:
		if m.node.UpdateCtx != nil {
			first := true
			for _, ctx := range m.node.UpdateCtx.Ref {
				if !first {
					buf.WriteString(", ")
				}
				buf.WriteString(ctx.SchemaName + "." + ctx.ObjName)
				if first {
					first = false
				}
			}
		} else {
			return "", moerr.NewInvalidInput(ctx, "Table definition not found when plan is serialized to json")
		}
	case plan.Node_DELETE:
		if m.node.DeleteCtx != nil {
			first := true
			for _, ctx := range m.node.DeleteCtx.Ref {
				if !first {
					buf.WriteString(", ")
				}
				buf.WriteString(ctx.SchemaName + "." + ctx.ObjName)
				if first {
					first = false
				}
			}
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
	default:
		return "", moerr.NewInternalError(ctx, "Unsupported node type when plan is serialized to json")
	}
	return strings.TrimSpace(buf.String()), nil
}

func (m MarshalNodeImpl) GetNodeLabels(ctx context.Context, options *ExplainOptions) ([]Label, error) {
	labels := make([]Label, 0)

	switch m.node.NodeType {
	case plan.Node_TABLE_SCAN, plan.Node_FUNCTION_SCAN, plan.Node_EXTERNAL_SCAN,
		plan.Node_MATERIAL_SCAN:
		tableDef := m.node.TableDef
		objRef := m.node.ObjRef
		var fullTableName string
		if objRef != nil {
			fullTableName += objRef.GetSchemaName() + "." + objRef.GetObjName()
		} else if tableDef != nil {
			fullTableName += tableDef.GetName()
		} else {
			return nil, moerr.NewInternalError(ctx, "Table definition not found when plan is serialized to json")
		}

		labels = append(labels, Label{
			Name:  "Full table name",
			Value: fullTableName,
		})

		// "name" : "Columns (2 / 28)",
		columns := GetTableColsLableValue(ctx, tableDef.Cols, options)

		labels = append(labels, Label{
			Name:  "Columns",
			Value: columns,
		})

		labels = append(labels, Label{
			Name:  "Total columns",
			Value: len(tableDef.Name2ColIndex),
		})

		labels = append(labels, Label{
			Name:  "Scan columns",
			Value: len(tableDef.Cols),
		})

	case plan.Node_INSERT:
		tableDef := m.node.TableDef
		objRef := m.node.ObjRef
		var fullTableName string
		if objRef != nil {
			fullTableName += objRef.GetSchemaName() + "." + objRef.GetObjName()
		} else if tableDef != nil {
			fullTableName += tableDef.GetName()
		} else {
			return nil, moerr.NewInternalError(ctx, "Table definition not found when plan is serialized to json")
		}

		labels = append(labels, Label{
			Name:  "Full table name",
			Value: fullTableName,
		})

		// "name" : "Columns (2 / 28)",
		columns := GetTableColsLableValue(ctx, tableDef.Cols, options)

		labels = append(labels, Label{
			Name:  "Columns",
			Value: columns,
		})

		labels = append(labels, Label{
			Name:  "Total columns",
			Value: len(tableDef.Cols),
		})

		labels = append(labels, Label{
			Name:  "Scan columns",
			Value: len(tableDef.Cols),
		})
	case plan.Node_UPDATE:
		if m.node.UpdateCtx != nil {
			updateTableNames := GetUpdateTableLableValue(ctx, m.node.UpdateCtx, options)
			labels = append(labels, Label{
				Name:  "Full table name",
				Value: updateTableNames,
			})

			updateCols := make([]string, 0)
			for i, ctx := range m.node.UpdateCtx.Ref {
				if m.node.UpdateCtx.UpdateCol[i] != nil {
					upcols := GetUpdateTableColsLableValue(m.node.UpdateCtx.UpdateCol[i].Map, ctx.SchemaName, ctx.ObjName, options)
					updateCols = append(updateCols, upcols...)
				}
			}
			labels = append(labels, Label{
				Name:  "Update columns",
				Value: updateCols,
			})
		} else {
			return nil, moerr.NewInvalidInput(ctx, "Table definition not found when plan is serialized to json")
		}
	case plan.Node_DELETE:
		if m.node.DeleteCtx != nil {
			deleteTableNames := GetDeleteTableLableValue(ctx, m.node.DeleteCtx, options)
			labels = append(labels, Label{
				Name:  "Full table name",
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
		labels = append(labels, Label{
			Name:  "List of expressions",
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
			labels = append(labels, Label{
				Name:  "Grouping keys",
				Value: value,
			})
		}

		// Get Aggregate function info
		if len(m.node.AggList) > 0 {
			value, err := GetExprsLabelValue(ctx, m.node.AggList, options)
			if err != nil {
				return nil, err
			}
			labels = append(labels, Label{
				Name:  "Aggregate functions",
				Value: value,
			})
		}
	case plan.Node_FILTER:
		value, err := GetExprsLabelValue(ctx, m.node.FilterList, options)
		if err != nil {
			return nil, err
		}
		labels = append(labels, Label{
			Name:  "Filter conditions",
			Value: value,
		})
	case plan.Node_JOIN:
		// Get Join type
		labels = append(labels, Label{
			Name:  "Join type",
			Value: m.node.JoinType.String(),
		})

		// Get Join Condition info
		if len(m.node.OnList) > 0 {
			value, err := GetExprsLabelValue(ctx, m.node.OnList, options)
			if err != nil {
				return nil, err
			}
			labels = append(labels, Label{
				Name:  "Join conditions",
				Value: value,
			})
		}
		labels = append(labels, Label{
			Name:  "Left node id",
			Value: m.node.Children[0],
		})
		labels = append(labels, Label{
			Name:  "Right node id",
			Value: m.node.Children[1],
		})
	case plan.Node_SORT:
		result, err := GettOrderByLabelValue(ctx, m.node.OrderBy, options)
		if err != nil {
			return nil, err
		}
		labels = append(labels, Label{
			Name:  "Sort keys",
			Value: result,
		})
	case plan.Node_VALUE_SCAN:
		value, err := GetExprsLabelValue(ctx, m.node.ProjectList, options)
		if err != nil {
			return nil, err
		}
		labels = append(labels, Label{
			Name:  "List of values",
			Value: value,
		})
	case plan.Node_UNION:
		value, err := GetExprsLabelValue(ctx, m.node.ProjectList, options)
		if err != nil {
			return nil, err
		}
		labels = append(labels, Label{
			Name:  "Union expressions",
			Value: value,
		})
	case plan.Node_UNION_ALL:
		value, err := GetExprsLabelValue(ctx, m.node.ProjectList, options)
		if err != nil {
			return nil, err
		}
		labels = append(labels, Label{
			Name:  "Union all expressions",
			Value: value,
		})
	case plan.Node_INTERSECT:
		value, err := GetExprsLabelValue(ctx, m.node.ProjectList, options)
		if err != nil {
			return nil, err
		}
		labels = append(labels, Label{
			Name:  "Intersect expressions",
			Value: value,
		})
	case plan.Node_INTERSECT_ALL:
		value, err := GetExprsLabelValue(ctx, m.node.ProjectList, options)
		if err != nil {
			return nil, err
		}
		labels = append(labels, Label{
			Name:  "Intersect All expressions",
			Value: value,
		})
	case plan.Node_MINUS:
		value, err := GetExprsLabelValue(ctx, m.node.ProjectList, options)
		if err != nil {
			return nil, err
		}
		labels = append(labels, Label{
			Name:  "Minus expressions",
			Value: value,
		})
	default:
		return nil, moerr.NewInternalError(ctx, "Unsupported node type when plan is serialized to json")
	}

	if m.node.NodeType != plan.Node_FILTER && m.node.FilterList != nil {
		// Where condition
		value, err := GetExprsLabelValue(ctx, m.node.FilterList, options)
		if err != nil {
			return nil, err
		}
		labels = append(labels, Label{
			Name:  "Filter conditions",
			Value: value,
		})
	}

	// Get Limit And Offset info
	if m.node.Limit != nil {
		buf := bytes.NewBuffer(make([]byte, 0, 80))
		err := describeExpr(ctx, m.node.Limit, options, buf)
		if err != nil {
			return nil, err
		}
		labels = append(labels, Label{
			Name:  "Number of rows",
			Value: buf.String(),
		})

		if m.node.Offset != nil {
			buf.Reset()
			err := describeExpr(ctx, m.node.Offset, options, buf)
			if err != nil {
				return nil, err
			}
			labels = append(labels, Label{
				Name:  "Offset",
				Value: buf.String(),
			})
		} else {
			labels = append(labels, Label{
				Name:  "Offset",
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
const S3IOByte = "S3 IO Byte"
const S3IOInputCount = "S3 IO Input Count"
const S3IOOutputCount = "S3 IO Output Count"
const Network = "Network"

func (m MarshalNodeImpl) GetStatistics(ctx context.Context, options *ExplainOptions) Statistics {
	statistics := NewStatistics()
	if options.Analyze && m.node.AnalyzeInfo != nil {
		analyzeInfo := m.node.AnalyzeInfo
		times := []StatisticValue{
			{
				Name:  TimeConsumed,
				Value: analyzeInfo.TimeConsumed,
				Unit:  "ns",
			},
			{
				Name:  WaitTime,
				Value: analyzeInfo.WaitTimeConsumed,
				Unit:  "ns",
			},
			{
				Name:  ScanTime,
				Value: analyzeInfo.ScanTime,
				Unit:  "ns",
			},
			{
				Name:  InsertTime,
				Value: analyzeInfo.InsertTime,
				Unit:  "ns",
			},
		}
		mbps := []StatisticValue{
			{
				Name:  InputRows,
				Value: analyzeInfo.InputRows,
				Unit:  "count",
			},
			{
				Name:  OutputRows,
				Value: analyzeInfo.OutputRows,
				Unit:  "count",
			},
			{
				Name:  InputSize,
				Value: analyzeInfo.InputSize,
				Unit:  "byte",
			},
			{
				Name:  OutputSize,
				Value: analyzeInfo.OutputSize,
				Unit:  "byte",
			},
		}

		mems := []StatisticValue{
			{
				Name:  MemorySize,
				Value: analyzeInfo.MemorySize,
				Unit:  "byte",
			},
		}

		io := []StatisticValue{
			{
				Name:  DiskIO,
				Value: analyzeInfo.DiskIO,
				Unit:  "byte",
			},
			{
				Name:  S3IOByte,
				Value: analyzeInfo.S3IOByte,
				Unit:  "byte",
			},
			{
				Name:  S3IOInputCount,
				Value: analyzeInfo.S3IOInputCount,
				Unit:  "count",
			},
			{
				Name:  S3IOOutputCount,
				Value: analyzeInfo.S3IOOutputCount,
				Unit:  "count",
			},
		}

		nw := []StatisticValue{
			{
				Name:  Network,
				Value: analyzeInfo.NetworkIO,
				Unit:  "byte",
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

func (m MarshalNodeImpl) GetTotalStats() TotalStats {
	totalStats := TotalStats{
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

func GettOrderByLabelValue(ctx context.Context, orderbyList []*plan.OrderBySpec, options *ExplainOptions) ([]string, error) {
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

func GetDeleteTableLableValue(ctx context.Context, deleteCtx *plan.DeleteCtx, options *ExplainOptions) []string {
	if deleteCtx == nil {
		return make([]string, 0)
	}
	result := make([]string, 0, len(deleteCtx.Ref))
	for _, ctx := range deleteCtx.Ref {
		result = append(result, ctx.SchemaName+"."+ctx.ObjName)
	}
	return result
}

func GetUpdateTableLableValue(ctx context.Context, updateCtx *plan.UpdateCtx, options *ExplainOptions) []string {
	if updateCtx == nil {
		return make([]string, 0)
	}
	result := make([]string, 0, len(updateCtx.Ref))
	for _, ctx := range updateCtx.Ref {
		result = append(result, ctx.SchemaName+"."+ctx.ObjName)
	}
	return result
}

func GetTableColsLableValue(ctx context.Context, cols []*plan.ColDef, options *ExplainOptions) []string {
	columns := make([]string, len(cols))
	for i, col := range cols {
		columns[i] = col.Name
	}
	return columns
}

func GetUpdateTableColsLableValue(cols map[string]int32, db string, tname string, options *ExplainOptions) []string {
	columns := make([]string, len(cols))
	i := 0
	for col := range cols {
		columns[i] = db + "." + tname + "." + col
		i++
	}
	return columns
}
