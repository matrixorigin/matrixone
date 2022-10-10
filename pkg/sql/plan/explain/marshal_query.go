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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"strconv"
	"strings"
)

func ConvertNode(node *plan.Node, options *ExplainOptions) (*Node, error) {
	marshalNodeImpl := NewMarshalNodeImpl(node)
	newNode := &Node{
		NodeId:     strconv.FormatInt(int64(node.NodeId), 10),
		Statistics: marshalNodeImpl.GetStatistics(options),
		Cost:       marshalNodeImpl.GetCost(),
		TotalStats: marshalNodeImpl.GetTotalStats(),
	}
	name, err := marshalNodeImpl.GetNodeName()
	if err != nil {
		return nil, err
	}
	newNode.Name = name

	title, err := marshalNodeImpl.GetNodeTitle(options)
	if err != nil {
		return nil, err
	}
	newNode.Title = title

	labels, err := marshalNodeImpl.GetNodeLabels(options)
	if err != nil {
		return nil, err
	}
	newNode.Labels = labels
	return newNode, nil
}

type MarshalNode interface {
	GetNodeName() (string, error)
	GetNodeTitle(options *ExplainOptions) (string, error)
	GetNodeLabels(options *ExplainOptions) ([]Label, error)
	GetStatistics(options *ExplainOptions) Statistics
	GetCost() Cost
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

func (m MarshalNodeImpl) GetCost() Cost {
	if m.node.Cost != nil {
		return Cost{
			Start:   m.node.Cost.Start,
			Total:   m.node.Cost.Total,
			Card:    m.node.Cost.Card,
			Ndv:     m.node.Cost.Ndv,
			Rowsize: m.node.Cost.Rowsize,
		}
	} else {
		return Cost{}
	}
}

func (m MarshalNodeImpl) GetNodeName() (string, error) {
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
		return name, moerr.NewInternalError("Unsupported node type when plan is serialized to json")
	}
	return name, nil
}

func (m MarshalNodeImpl) GetNodeTitle(options *ExplainOptions) (string, error) {
	var result string
	var err error
	switch m.node.NodeType {
	case plan.Node_TABLE_SCAN, plan.Node_EXTERNAL_SCAN, plan.Node_MATERIAL_SCAN, plan.Node_INSERT:
		//"title" : "SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.DATE_DIM",
		if m.node.ObjRef != nil {
			result += m.node.ObjRef.GetSchemaName() + "." + m.node.ObjRef.GetObjName()
		} else if m.node.TableDef != nil {
			result += m.node.TableDef.GetName()
		} else {
			return result, moerr.NewInvalidInput("Table definition not found when plan is serialized to json")
		}
	case plan.Node_UPDATE:
		if m.node.UpdateCtxs != nil {
			first := true
			for _, ctx := range m.node.UpdateCtxs {
				if !first {
					result += ", "
				}
				result += ctx.DbName + "." + ctx.TblName
				if first {
					first = false
				}
			}
		} else {
			return result, moerr.NewInvalidInput("Table definition not found when plan is serialized to json")
		}
	case plan.Node_DELETE:
		if m.node.DeleteTablesCtx != nil {
			first := true
			for _, ctx := range m.node.DeleteTablesCtx {
				if !first {
					result += ", "
				}
				result += ctx.DbName + "." + ctx.TblName
				if first {
					first = false
				}
			}
		} else {
			return result, moerr.NewInternalError("Table definition not found when plan is serialized to json")
		}
	case plan.Node_PROJECT, plan.Node_VALUE_SCAN, plan.Node_UNION, plan.Node_UNION_ALL,
		plan.Node_INTERSECT, plan.Node_INTERSECT_ALL, plan.Node_MINUS:
		//"title" : "STORE.S_STORE_NAME,STORE.S_STORE_ID,WSS.D_WEEK_SEQ"
		exprs := NewExprListDescribeImpl(m.node.ProjectList)
		result, err = exprs.GetDescription(options)
		if err != nil {
			return result, err
		}
	case plan.Node_AGG:
		// "SUM(IFF(DATE_DIM.D_DAY_NAME = 'Sunday', STORE_SALES.SS_SALES_PRICE, null))"
		exprs := NewExprListDescribeImpl(m.node.AggList)
		result, err = exprs.GetDescription(options)
		if err != nil {
			return result, err
		}
	case plan.Node_FILTER:
		//"title" : "(D_0.D_MONTH_SEQ >= 1189) AND (D_0.D_MONTH_SEQ <= 1200)",
		exprs := NewExprListDescribeImpl(m.node.FilterList)
		result, err = exprs.GetDescription(options)
		if err != nil {
			return result, err
		}
	case plan.Node_JOIN:
		//"title" : "(DATE_DIM.D_DATE_SK = STORE_SALES.SS_SOLD_DATE_SK)",
		exprs := NewExprListDescribeImpl(m.node.OnList)
		result, err = exprs.GetDescription(options)
		if err != nil {
			return result, err
		}
	case plan.Node_SORT:
		//"title" : "STORE.S_STORE_NAME ASC NULLS LAST,STORE.S_STORE_ID ASC NULLS LAST,WSS.D_WEEK_SEQ ASC NULLS LAST",
		orderByDescImpl := NewOrderByDescribeImpl(m.node.OrderBy)
		result, err = orderByDescImpl.GetDescription(options)
		if err != nil {
			return result, err
		}
	default:
		return "", moerr.NewInternalError("Unsupported node type when plan is serialized to json")
	}
	return strings.TrimSpace(result), nil
}

func (m MarshalNodeImpl) GetNodeLabels(options *ExplainOptions) ([]Label, error) {
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
			return nil, moerr.NewInternalError("Table definition not found when plan is serialized to json")
		}

		labels = append(labels, Label{
			Name:  "Full table name",
			Value: fullTableName,
		})

		// "name" : "Columns (2 / 28)",
		columns := GetTableColsLableValue(tableDef.Cols, options)

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
			return nil, moerr.NewInternalError("Table definition not found when plan is serialized to json")
		}

		labels = append(labels, Label{
			Name:  "Full table name",
			Value: fullTableName,
		})

		// "name" : "Columns (2 / 28)",
		columns := GetTableColsLableValue(tableDef.Cols, options)

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
		if m.node.UpdateCtxs != nil {
			updateTableNames := GetUpdateTableLableValue(m.node.UpdateCtxs, options)
			labels = append(labels, Label{
				Name:  "Full table name",
				Value: updateTableNames,
			})

			updateCols := make([]string, 0)
			for _, ctx := range m.node.UpdateCtxs {
				if ctx.UpdateCols != nil {
					upcols := GetUpdateTableColsLableValue(ctx.UpdateCols, ctx.DbName, ctx.TblName, options)
					updateCols = append(updateCols, upcols...)
				}
			}
			labels = append(labels, Label{
				Name:  "Update columns",
				Value: updateCols,
			})
		} else {
			return nil, moerr.NewInvalidInput("Table definition not found when plan is serialized to json")
		}
	case plan.Node_DELETE:
		if m.node.DeleteTablesCtx != nil {
			deleteTableNames := GetDeleteTableLableValue(m.node.DeleteTablesCtx, options)
			labels = append(labels, Label{
				Name:  "Full table name",
				Value: deleteTableNames,
			})
		} else {
			return nil, moerr.NewInvalidInput("Table definition not found when plan is serialized to json")
		}
	case plan.Node_PROJECT:
		value, err := GetExprsLabelValue(m.node.ProjectList, options)
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
			value, err := GetExprsLabelValue(m.node.GroupBy, options)
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
			value, err := GetExprsLabelValue(m.node.AggList, options)
			if err != nil {
				return nil, err
			}
			labels = append(labels, Label{
				Name:  "Aggregate functions",
				Value: value,
			})
		}
	case plan.Node_FILTER:
		value, err := GetExprsLabelValue(m.node.FilterList, options)
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
			value, err := GetExprsLabelValue(m.node.OnList, options)
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
		result, err := GettOrderByLabelValue(m.node.OrderBy, options)
		if err != nil {
			return nil, err
		}
		labels = append(labels, Label{
			Name:  "Sort keys",
			Value: result,
		})
	case plan.Node_VALUE_SCAN:
		value, err := GetExprsLabelValue(m.node.ProjectList, options)
		if err != nil {
			return nil, err
		}
		labels = append(labels, Label{
			Name:  "List of values",
			Value: value,
		})
	case plan.Node_UNION:
		value, err := GetExprsLabelValue(m.node.ProjectList, options)
		if err != nil {
			return nil, err
		}
		labels = append(labels, Label{
			Name:  "Union expressions",
			Value: value,
		})
	case plan.Node_UNION_ALL:
		value, err := GetExprsLabelValue(m.node.ProjectList, options)
		if err != nil {
			return nil, err
		}
		labels = append(labels, Label{
			Name:  "Union all expressions",
			Value: value,
		})
	case plan.Node_INTERSECT:
		value, err := GetExprsLabelValue(m.node.ProjectList, options)
		if err != nil {
			return nil, err
		}
		labels = append(labels, Label{
			Name:  "Intersect expressions",
			Value: value,
		})
	case plan.Node_INTERSECT_ALL:
		value, err := GetExprsLabelValue(m.node.ProjectList, options)
		if err != nil {
			return nil, err
		}
		labels = append(labels, Label{
			Name:  "Intersect All expressions",
			Value: value,
		})
	case plan.Node_MINUS:
		value, err := GetExprsLabelValue(m.node.ProjectList, options)
		if err != nil {
			return nil, err
		}
		labels = append(labels, Label{
			Name:  "Minus expressions",
			Value: value,
		})
	default:
		return nil, moerr.NewInternalError("Unsupported node type when plan is serialized to json")
	}

	if m.node.NodeType != plan.Node_FILTER && m.node.FilterList != nil {
		// Where condition
		value, err := GetExprsLabelValue(m.node.FilterList, options)
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
		limitInfo, err := describeExpr(m.node.Limit, options)
		if err != nil {
			return nil, err
		}
		labels = append(labels, Label{
			Name:  "Number of rows",
			Value: limitInfo,
		})

		if m.node.Offset != nil {
			offsetInfo, err := describeExpr(m.node.Offset, options)
			if err != nil {
				return nil, err
			}
			labels = append(labels, Label{
				Name:  "Offset",
				Value: offsetInfo,
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

const InputRows = "Input Rows"
const InputSize = "Input Size"

func (m MarshalNodeImpl) GetStatistics(options *ExplainOptions) Statistics {
	statistics := NewStatistics()
	if options.Analyze && m.node.AnalyzeInfo != nil {
		analyzeInfo := m.node.AnalyzeInfo
		mbps := []StatisticValue{
			{
				Name:  InputRows,
				Value: analyzeInfo.InputRows,
				Unit:  "count",
			},
			{
				Name:  "Output Rows",
				Value: analyzeInfo.OutputRows,
				Unit:  "count",
			},
			{
				Name:  InputSize,
				Value: analyzeInfo.InputSize,
				Unit:  "byte",
			},
			{
				Name:  "Output Size",
				Value: analyzeInfo.OutputSize,
				Unit:  "byte",
			},
		}

		mems := []StatisticValue{
			{
				Name:  "Memory Size",
				Value: analyzeInfo.MemorySize,
				Unit:  "byte",
			},
		}
		statistics.Throughput = append(statistics.Throughput, mbps...)
		statistics.Memory = append(statistics.Memory, mems...)
	}
	return *statistics
}

func (m MarshalNodeImpl) GetTotalStats() TotalStats {
	totalStats := TotalStats{
		Name: "Time spent",
		Unit: "us",
	}
	if m.node.AnalyzeInfo != nil {
		totalStats.Value = m.node.AnalyzeInfo.TimeConsumed
	} else {
		totalStats.Value = 0
	}
	return totalStats
}

var _ MarshalNode = MarshalNodeImpl{}

func GetExprsLabelValue(exprList []*plan.Expr, options *ExplainOptions) ([]string, error) {
	if exprList == nil {
		return make([]string, 0), nil
	}
	result := make([]string, 0)
	for _, v := range exprList {
		descV, err := describeExpr(v, options)
		if err != nil {
			return result, err
		}
		result = append(result, descV)
	}
	return result, nil
}

func GettOrderByLabelValue(orderbyList []*plan.OrderBySpec, options *ExplainOptions) ([]string, error) {
	if orderbyList == nil {
		return make([]string, 0), nil
	}
	result := make([]string, 0)
	for _, v := range orderbyList {
		descExpr, err := describeExpr(v.Expr, options)
		if err != nil {
			return result, err
		}

		flagKey := int32(v.Flag)
		orderbyFlag := plan.OrderBySpec_OrderByFlag_name[flagKey]
		result = append(result, descExpr+" "+orderbyFlag)
	}
	return result, nil
}

func GetDeleteTableLableValue(deleteCtxs []*plan.DeleteTableCtx, options *ExplainOptions) []string {
	if deleteCtxs == nil {
		return make([]string, 0)
	}
	result := make([]string, 0)
	for _, ctx := range deleteCtxs {
		result = append(result, ctx.DbName+"."+ctx.TblName)
	}
	return result
}

func GetUpdateTableLableValue(updateCtxs []*plan.UpdateCtx, options *ExplainOptions) []string {
	if updateCtxs == nil {
		return make([]string, 0)
	}
	result := make([]string, 0)
	for _, ctx := range updateCtxs {
		result = append(result, ctx.DbName+"."+ctx.TblName)
	}
	return result
}

func GetTableColsLableValue(cols []*plan.ColDef, options *ExplainOptions) []string {
	columns := make([]string, len(cols))
	for i, col := range cols {
		columns[i] = col.Name
	}
	return columns
}

func GetUpdateTableColsLableValue(cols []*plan.ColDef, db string, tname string, options *ExplainOptions) []string {
	columns := make([]string, len(cols))
	for i, col := range cols {
		columns[i] = db + "." + tname + "." + col.Name
	}
	return columns
}
