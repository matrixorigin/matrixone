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
)

func ConvertNode(node *plan.Node, options *ExplainOptions) (*Node, error) {
	marshalNodeImpl := NewMarshalNodeImpl(node)
	newNode := &Node{
		NodeId:     strconv.FormatInt(int64(node.NodeId), 10),
		Statistics: marshalNodeImpl.GetStatistics(),
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
	GetStatistics() Statistics
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
	c := m.node.Cost
	return Cost{
		Start:   c.Start,
		Total:   c.Total,
		Card:    c.Card,
		Ndv:     c.Ndv,
		Rowsize: c.Rowsize,
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
		return name, moerr.NewError(moerr.ERROR_SERIALIZE_PLAN_JSON, "Unsupported node type when plan is serialized to json")
	}
	return name, nil
}

func (m MarshalNodeImpl) GetNodeTitle(options *ExplainOptions) (string, error) {
	var result string
	var err error
	switch m.node.NodeType {
	case plan.Node_TABLE_SCAN, plan.Node_FUNCTION_SCAN, plan.Node_EXTERNAL_SCAN,
		plan.Node_MATERIAL_SCAN, plan.Node_INSERT, plan.Node_UPDATE, plan.Node_DELETE:
		//"title" : "SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.DATE_DIM",
		if m.node.ObjRef != nil {
			result += m.node.ObjRef.GetSchemaName() + "." + m.node.ObjRef.GetObjName()
		} else if m.node.TableDef != nil {
			result += m.node.TableDef.GetName()
		} else {
			return result, moerr.NewError(moerr.ERROR_SERIALIZE_PLAN_JSON, "Table definition not found when plan is serialized to json")
		}
	case plan.Node_PROJECT:
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
		first := true
		for _, v := range m.node.GetOrderBy() {
			if !first {
				result += ", "
			}
			first = false
			orderByDescImpl := NewOrderByDescribeImpl(v)
			describe, err := orderByDescImpl.GetDescription(options)
			if err != nil {
				return result, err
			}
			result += describe
		}
	case plan.Node_VALUE_SCAN:
		//"title" : "STORE.S_STORE_NAME,STORE.S_STORE_ID,WSS.D_WEEK_SEQ"
		exprs := NewExprListDescribeImpl(m.node.ProjectList)
		result, err = exprs.GetDescription(options)
		if err != nil {
			return result, err
		}
	case plan.Node_UNION:
		//"title" : "STORE.S_STORE_NAME,STORE.S_STORE_ID,WSS.D_WEEK_SEQ"
		exprs := NewExprListDescribeImpl(m.node.ProjectList)
		result, err = exprs.GetDescription(options)
		if err != nil {
			return result, err
		}
	case plan.Node_UNION_ALL:
		//"title" : "STORE.S_STORE_NAME,STORE.S_STORE_ID,WSS.D_WEEK_SEQ"
		exprs := NewExprListDescribeImpl(m.node.ProjectList)
		result, err = exprs.GetDescription(options)
		if err != nil {
			return result, err
		}
	case plan.Node_INTERSECT:
		exprs := NewExprListDescribeImpl(m.node.ProjectList)
		result, err = exprs.GetDescription(options)
		if err != nil {
			return result, err
		}
	case plan.Node_INTERSECT_ALL:
		exprs := NewExprListDescribeImpl(m.node.ProjectList)
		result, err = exprs.GetDescription(options)
		if err != nil {
			return result, err
		}
	case plan.Node_MINUS:
		exprs := NewExprListDescribeImpl(m.node.ProjectList)
		result, err = exprs.GetDescription(options)
		if err != nil {
			return result, err
		}
	default:
		return "", moerr.NewError(moerr.ERROR_SERIALIZE_PLAN_JSON, "Unsupported node type when plan is serialized to json")
	}
	return result, nil
}

func (m MarshalNodeImpl) GetNodeLabels(options *ExplainOptions) ([]Label, error) {
	labels := make([]Label, 0)

	switch m.node.NodeType {
	case plan.Node_TABLE_SCAN, plan.Node_FUNCTION_SCAN, plan.Node_EXTERNAL_SCAN,
		plan.Node_MATERIAL_SCAN, plan.Node_INSERT, plan.Node_UPDATE, plan.Node_DELETE:
		tableDef := m.node.TableDef
		objRef := m.node.ObjRef
		var fullTableName string
		if objRef != nil {
			fullTableName += objRef.GetSchemaName() + "." + objRef.GetObjName()
		} else if tableDef != nil {
			fullTableName += tableDef.GetName()
		} else {
			return nil, moerr.NewError(moerr.ERROR_SERIALIZE_PLAN_JSON, "Table definition not found when plan is serialized to json")
		}
		// "name" : "Columns (2 / 28)",
		columns := make([]string, len(tableDef.Cols))
		for i, col := range tableDef.Cols {
			columns[i] = col.Name
		}

		labels = append(labels, Label{
			Name:  "Full table name",
			Value: fullTableName,
		})

		labels = append(labels, Label{
			Name:  "Columns",
			Value: columns,
		})
	case plan.Node_PROJECT:
		value, err := GetLabelValue(m.node.ProjectList, options)
		if err != nil {
			return nil, err
		}
		labels = append(labels, Label{
			Name:  "List of Expressions",
			Value: value,
		})
	case plan.Node_AGG:
		// Get Group key info
		if len(m.node.GroupBy) > 0 {
			// Get Grouping Key
			value, err := GetLabelValue(m.node.GroupBy, options)
			if err != nil {
				return nil, err
			}
			labels = append(labels, Label{
				Name:  "Grouping Keys",
				Value: value,
			})
		}

		// Get Aggregate function info
		if len(m.node.AggList) > 0 {
			value, err := GetLabelValue(m.node.AggList, options)
			if err != nil {
				return nil, err
			}
			labels = append(labels, Label{
				Name:  "Aggregate Functions",
				Value: value,
			})
		}
	case plan.Node_FILTER:
		value, err := GetLabelValue(m.node.FilterList, options)
		if err != nil {
			return nil, err
		}
		labels = append(labels, Label{
			Name:  "Filter condition",
			Value: value,
		})
	case plan.Node_JOIN:
		// Get Join type
		labels = append(labels, Label{
			Name:  "Join Type",
			Value: m.node.JoinType.String(),
		})

		// Get Join Condition info
		if len(m.node.OnList) > 0 {
			value, err := GetLabelValue(m.node.OnList, options)
			if err != nil {
				return nil, err
			}
			labels = append(labels, Label{
				Name:  "Join Condition",
				Value: value,
			})
		}
		labels = append(labels, Label{
			Name:  "Left Node Id",
			Value: m.node.Children[0],
		})
		labels = append(labels, Label{
			Name:  "Right Node Id",
			Value: m.node.Children[1],
		})
	case plan.Node_SORT:
		result := make([]string, 0)
		for _, v := range m.node.GetOrderBy() {
			orderByDescImpl := NewOrderByDescribeImpl(v)
			describe, err := orderByDescImpl.GetDescription(options)
			if err != nil {
				return nil, err
			}
			result = append(result, describe)
		}
		labels = append(labels, Label{
			Name:  "Sort keys",
			Value: result,
		})
	case plan.Node_VALUE_SCAN:
		value, err := GetLabelValue(m.node.ProjectList, options)
		if err != nil {
			return nil, err
		}
		labels = append(labels, Label{
			Name:  "List of Values",
			Value: value,
		})
	case plan.Node_UNION:
		value, err := GetLabelValue(m.node.ProjectList, options)
		if err != nil {
			return nil, err
		}
		labels = append(labels, Label{
			Name:  "Union expressions",
			Value: value,
		})
	case plan.Node_UNION_ALL:
		value, err := GetLabelValue(m.node.ProjectList, options)
		if err != nil {
			return nil, err
		}
		labels = append(labels, Label{
			Name:  "Union All expressions",
			Value: value,
		})
	case plan.Node_INTERSECT:
		value, err := GetLabelValue(m.node.ProjectList, options)
		if err != nil {
			return nil, err
		}
		labels = append(labels, Label{
			Name:  "Intersect expressions",
			Value: value,
		})
	case plan.Node_INTERSECT_ALL:
		value, err := GetLabelValue(m.node.ProjectList, options)
		if err != nil {
			return nil, err
		}
		labels = append(labels, Label{
			Name:  "Intersect All expressions",
			Value: value,
		})
	case plan.Node_MINUS:
		value, err := GetLabelValue(m.node.ProjectList, options)
		if err != nil {
			return nil, err
		}
		labels = append(labels, Label{
			Name:  "Minus expressions",
			Value: value,
		})
	default:
		return nil, moerr.NewError(moerr.ERROR_SERIALIZE_PLAN_JSON, "Unsupported node type when plan is serialized to json")
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

func (m MarshalNodeImpl) GetStatistics() Statistics {
	analyzeInfo := m.node.AnalyzeInfo
	mbps := []StatisticValue{
		{
			Name:  "Input Rows",
			Value: analyzeInfo.InputRows,
			Unit:  "count",
		},
		{
			Name:  "Output Rows",
			Value: analyzeInfo.OutputRows,
			Unit:  "count",
		},
		{
			Name:  "Input Size",
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

	statistics := NewStatistics()
	statistics.Throughput = append(statistics.Throughput, mbps...)
	statistics.Memory = append(statistics.Memory, mems...)
	return *statistics
}

func (m MarshalNodeImpl) GetTotalStats() TotalStats {
	analyzeInfo := m.node.AnalyzeInfo
	return TotalStats{
		Name:  "Time spent",
		Value: analyzeInfo.TimeConsumed,
		Unit:  "us",
	}
}

var _ MarshalNode = MarshalNodeImpl{}

func GetLabelValue(exprList []*plan.Expr, options *ExplainOptions) ([]string, error) {
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
