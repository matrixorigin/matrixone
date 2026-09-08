// Copyright 2026 Matrix Origin
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
	"encoding/json"
	"math"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

type sqlJSONPlan struct {
	QueryBlock sqlJSONQueryBlock `json:"query_block"`
	MatrixOne  sqlJSONMatrixOne  `json:"matrixone"`
}

type sqlJSONQueryBlock struct {
	SelectID int           `json:"select_id"`
	Table    *sqlJSONTable `json:"table,omitempty"`
}

type sqlJSONTable struct {
	TableName         string `json:"table_name"`
	AttachedCondition string `json:"attached_condition,omitempty"`
}

type sqlJSONMatrixOne struct {
	SchemaVersion int           `json:"schema_version"`
	StatementType string        `json:"statement_type"`
	Steps         []sqlJSONStep `json:"steps"`
	Nodes         []sqlJSONNode `json:"nodes"`
	Edges         []sqlJSONEdge `json:"edges"`
}

type sqlJSONStep struct {
	Step int    `json:"step"`
	Root string `json:"root"`
}

type sqlJSONNode struct {
	ID          string             `json:"id"`
	Operator    string             `json:"operator"`
	Inputs      []string           `json:"inputs"`
	TableName   string             `json:"table_name,omitempty"`
	TableNames  []string           `json:"table_names,omitempty"`
	Filter      string             `json:"filter,omitempty"`
	JoinType    string             `json:"join_type,omitempty"`
	Join        string             `json:"join_condition,omitempty"`
	GroupBy     string             `json:"group_by,omitempty"`
	Aggregate   string             `json:"aggregate,omitempty"`
	OrderBy     string             `json:"order_by,omitempty"`
	Expressions []string           `json:"expressions,omitempty"`
	SourceSteps []int32            `json:"source_steps,omitempty"`
	Statistics  map[string]float64 `json:"statistics,omitempty"`
}

type sqlJSONEdge struct {
	From string `json:"from"`
	To   string `json:"to"`
}

type sqlJSONPlanBuilder struct {
	ctx        context.Context
	query      *plan.Query
	textOpts   ExplainOptions
	byID       map[int32]*plan.Node
	state      map[int32]uint8
	duplicates map[int32]struct{}
	nodes      []sqlJSONNode
	edges      []sqlJSONEdge
}

// BuildSQLJSONPlan serializes a query plan for the SQL EXPLAIN FORMAT=JSON
// interface. It intentionally uses a separate model from the internal
// diagnostic/UI JSON schema so the SQL contract can evolve independently.
func BuildSQLJSONPlan(ctx context.Context, query *plan.Query) ([]byte, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if query == nil {
		return nil, moerr.NewInvalidInput(ctx, "query plan is nil")
	}

	builder := &sqlJSONPlanBuilder{
		ctx:        ctx,
		query:      query,
		textOpts:   ExplainOptions{Format: EXPLAIN_FORMAT_TEXT},
		byID:       make(map[int32]*plan.Node, len(query.Nodes)),
		state:      make(map[int32]uint8, len(query.Nodes)),
		duplicates: make(map[int32]struct{}),
	}
	for _, node := range query.Nodes {
		if node == nil {
			continue
		}
		if _, exists := builder.byID[node.NodeId]; exists {
			builder.duplicates[node.NodeId] = struct{}{}
			continue
		}
		builder.byID[node.NodeId] = node
	}

	result := sqlJSONPlan{
		QueryBlock: sqlJSONQueryBlock{SelectID: 1},
		MatrixOne: sqlJSONMatrixOne{
			SchemaVersion: 1,
			StatementType: query.GetStmtType().String(),
			Steps:         make([]sqlJSONStep, 0, len(query.Steps)),
			Nodes:         make([]sqlJSONNode, 0, len(query.Nodes)),
			Edges:         make([]sqlJSONEdge, 0),
		},
	}
	for step, rootRef := range query.Steps {
		root := builder.resolveRoot(rootRef)
		if root == nil {
			return nil, moerr.NewInvalidInputf(ctx, "plan step %d references node %d", step, rootRef)
		}
		if err := builder.visit(root); err != nil {
			return nil, err
		}
		result.MatrixOne.Steps = append(result.MatrixOne.Steps, sqlJSONStep{
			Step: step,
			Root: strconv.FormatInt(int64(root.NodeId), 10),
		})
	}
	result.MatrixOne.Nodes = builder.nodes
	result.MatrixOne.Edges = builder.edges
	table, err := builder.singleTable()
	if err != nil {
		return nil, err
	}
	result.QueryBlock.Table = table
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	data, err := json.Marshal(result)
	if err != nil {
		return nil, moerr.NewInternalErrorf(ctx, "failed to serialize EXPLAIN FORMAT=JSON: %v", err)
	}
	return data, nil
}

func (b *sqlJSONPlanBuilder) resolveRoot(ref int32) *plan.Node {
	if ref < 0 || int(ref) >= len(b.query.Nodes) {
		return nil
	}
	return b.query.Nodes[ref]
}

func (b *sqlJSONPlanBuilder) resolveChild(ref int32) *plan.Node {
	if _, duplicate := b.duplicates[ref]; duplicate {
		return nil
	}
	return b.byID[ref]
}

func (b *sqlJSONPlanBuilder) visit(node *plan.Node) error {
	if node == nil {
		return nil
	}
	if _, duplicate := b.duplicates[node.NodeId]; duplicate {
		return moerr.NewInvalidInputf(b.ctx, "duplicate reachable plan node id %d", node.NodeId)
	}
	if err := b.ctx.Err(); err != nil {
		return err
	}
	switch b.state[node.NodeId] {
	case 1:
		return moerr.NewInvalidInputf(b.ctx, "cycle in plan at node %d", node.NodeId)
	case 2:
		return nil
	}
	b.state[node.NodeId] = 1

	item := sqlJSONNode{
		ID:          strconv.FormatInt(int64(node.NodeId), 10),
		Operator:    b.operator(node),
		Inputs:      make([]string, 0, len(node.Children)),
		Statistics:  finiteNodeStatistics(node),
		SourceSteps: append([]int32(nil), node.SourceStep...),
	}
	if err := b.addNodeDetails(&item, node); err != nil {
		return err
	}
	nodeIndex := len(b.nodes)
	b.nodes = append(b.nodes, item)

	for _, childRef := range node.Children {
		if _, duplicate := b.duplicates[childRef]; duplicate {
			return moerr.NewInvalidInputf(b.ctx, "ambiguous child node id %d", childRef)
		}
		child := b.resolveChild(childRef)
		if child == nil {
			return moerr.NewInvalidInputf(b.ctx, "node %d references missing child %d", node.NodeId, childRef)
		}
		childID := strconv.FormatInt(int64(child.NodeId), 10)
		item.Inputs = append(item.Inputs, childID)
		b.edges = append(b.edges, sqlJSONEdge{From: childID, To: item.ID})
		if err := b.visit(child); err != nil {
			return err
		}
	}
	b.nodes[nodeIndex].Inputs = item.Inputs
	b.state[node.NodeId] = 2
	return nil
}

func (b *sqlJSONPlanBuilder) operator(node *plan.Node) string {
	name, err := NewMarshalNodeImpl(node).GetNodeName(b.ctx)
	if err == nil && name != "" {
		return name
	}
	if name := node.NodeType.String(); name != "" {
		return name
	}
	return "UNKNOWN"
}

func (b *sqlJSONPlanBuilder) addNodeDetails(item *sqlJSONNode, node *plan.Node) error {
	switch node.NodeType {
	case plan.Node_TABLE_SCAN, plan.Node_EXTERNAL_SCAN, plan.Node_MATERIAL_SCAN:
		item.TableName = sqlJSONTableName(node)
	case plan.Node_FILTER, plan.Node_ASSERT:
		var err error
		item.Filter, err = sqlJSONExprList(b.ctx, node.FilterList, &b.textOpts)
		if err != nil {
			return err
		}
	case plan.Node_JOIN:
		item.JoinType = node.JoinType.String()
		var err error
		item.Join, err = sqlJSONExprList(b.ctx, node.OnList, &b.textOpts)
		if err != nil {
			return err
		}
	case plan.Node_AGG:
		var err error
		item.GroupBy, err = sqlJSONExprList(b.ctx, node.GroupBy, &b.textOpts)
		if err != nil {
			return err
		}
		item.Aggregate, err = sqlJSONExprList(b.ctx, node.AggList, &b.textOpts)
		if err != nil {
			return err
		}
	case plan.Node_SORT:
		buf := bytes.NewBuffer(make([]byte, 0, 128))
		if err := NewOrderByDescribeImpl(node.OrderBy).GetDescription(b.ctx, &b.textOpts, buf); err == nil {
			item.OrderBy = strings.TrimSpace(buf.String())
		} else {
			return err
		}
		limits := make([]*plan.Expr, 0, 2)
		if node.Limit != nil {
			limits = append(limits, node.Limit)
		}
		if node.Offset != nil {
			limits = append(limits, node.Offset)
		}
		var err error
		item.Expressions, err = sqlJSONExprValues(b.ctx, limits, &b.textOpts)
		if err != nil {
			return err
		}
	case plan.Node_WINDOW:
		var err error
		item.Expressions, err = sqlJSONExprValues(b.ctx, node.WinSpecList, &b.textOpts)
		if err != nil {
			return err
		}
	case plan.Node_TIME_WINDOW:
		var err error
		exprs := make([]*plan.Expr, 0, len(node.TimeWindowPartitionBy)+5)
		exprs = append(exprs, node.TimeWindowPartitionBy...)
		exprs = append(exprs, node.Interval, node.Sliding, node.Timestamp, node.WEnd,
			node.GapFillStart, node.GapFillEnd)
		item.Expressions, err = sqlJSONExprValues(b.ctx, exprs, &b.textOpts)
		if err != nil {
			return err
		}
	case plan.Node_FILL:
		var err error
		item.Expressions, err = sqlJSONExprValues(b.ctx, node.FillVal, &b.textOpts)
		if err != nil {
			return err
		}
	case plan.Node_PARTITION:
		buf := bytes.NewBuffer(make([]byte, 0, 128))
		if err := NewOrderByDescribeImpl(node.OrderBy).GetDescription(b.ctx, &b.textOpts, buf); err != nil {
			return err
		}
		item.OrderBy = strings.TrimSpace(buf.String())
		limits := make([]*plan.Expr, 0, 2)
		if node.Limit != nil {
			limits = append(limits, node.Limit)
		}
		if node.Offset != nil {
			limits = append(limits, node.Offset)
		}
		var err error
		item.Expressions, err = sqlJSONExprValues(b.ctx, limits, &b.textOpts)
		if err != nil {
			return err
		}
	case plan.Node_PROJECT, plan.Node_VALUE_SCAN, plan.Node_UNION, plan.Node_UNION_ALL,
		plan.Node_INTERSECT, plan.Node_INTERSECT_ALL, plan.Node_MINUS, plan.Node_MINUS_ALL,
		plan.Node_FUNCTION_SCAN, plan.Node_EXTERNAL_FUNCTION:
		var err error
		exprs := node.ProjectList
		if node.NodeType == plan.Node_FUNCTION_SCAN || node.NodeType == plan.Node_EXTERNAL_FUNCTION {
			exprs = node.TblFuncExprList
		}
		item.Expressions, err = sqlJSONExprValues(b.ctx, exprs, &b.textOpts)
		if err != nil {
			return err
		}
	case plan.Node_INSERT:
		if node.InsertCtx != nil {
			item.TableName = sqlJSONTargetName(node.InsertCtx.Ref, node.InsertCtx.TableDef)
		}
	case plan.Node_DELETE:
		if node.DeleteCtx != nil {
			item.TableName = sqlJSONTargetName(node.DeleteCtx.Ref, node.DeleteCtx.TableDef)
		}
	case plan.Node_PRE_INSERT:
		if node.PreInsertCtx != nil {
			item.TableName = sqlJSONTargetName(node.PreInsertCtx.Ref, node.PreInsertCtx.TableDef)
			var err error
			item.Expressions, err = sqlJSONExprValues(b.ctx,
				[]*plan.Expr{node.PreInsertCtx.CompPkeyExpr, node.PreInsertCtx.ClusterByExpr},
				&b.textOpts)
			if err != nil {
				return err
			}
		}
	case plan.Node_MULTI_UPDATE:
		for _, update := range node.UpdateCtxList {
			if update == nil {
				continue
			}
			if name := sqlJSONTargetName(update.ObjRef, update.TableDef); name != "" {
				item.TableNames = append(item.TableNames, name)
			}
		}
	case plan.Node_POSTDML:
		if node.PostDmlCtx != nil {
			item.TableName = sqlJSONObjectRefName(node.PostDmlCtx.Ref)
		}
	}
	return nil
}

func (b *sqlJSONPlanBuilder) singleTable() (*sqlJSONTable, error) {
	var scan *plan.Node
	for _, node := range b.nodes {
		id, err := strconv.ParseInt(node.ID, 10, 32)
		if err != nil {
			return nil, err
		}
		candidate := b.byID[int32(id)]
		if !sqlJSONIsTableScan(candidate) {
			continue
		}
		if scan != nil {
			return nil, nil
		}
		scan = candidate
	}
	if scan == nil {
		return nil, nil
	}
	tableName := sqlJSONTableName(scan)
	if tableName == "" {
		return nil, nil
	}
	table := &sqlJSONTable{TableName: tableName}
	if filter, err := sqlJSONExprList(b.ctx, scan.FilterList, &b.textOpts); err != nil {
		return nil, err
	} else {
		// A predicate is attached to the MySQL table only when the typed plan
		// records it on the scan itself. Standalone FILTER nodes remain in the
		// MatrixOne graph because their relationship to a table may include
		// projections, joins, or other operators.
		table.AttachedCondition = filter
	}
	return table, nil
}

func sqlJSONIsTableScan(node *plan.Node) bool {
	if node == nil {
		return false
	}
	switch node.NodeType {
	case plan.Node_TABLE_SCAN, plan.Node_EXTERNAL_SCAN, plan.Node_MATERIAL_SCAN:
		return true
	default:
		return false
	}
}

func finiteNodeStatistics(node *plan.Node) map[string]float64 {
	if node == nil || node.Stats == nil {
		return nil
	}
	stats := make(map[string]float64, 3)
	appendFinite := func(name string, value float64) {
		if !math.IsNaN(value) && !math.IsInf(value, 0) {
			stats[name] = value
		}
	}
	appendFinite("cost", node.Stats.Cost)
	appendFinite("estimated_rows", node.Stats.Outcnt)
	appendFinite("row_size", node.Stats.Rowsize)
	if len(stats) == 0 {
		return nil
	}
	return stats
}

func sqlJSONExprValues(ctx context.Context, exprs []*plan.Expr, options *ExplainOptions) ([]string, error) {
	values := make([]string, 0, len(exprs))
	for _, expr := range exprs {
		value, err := sqlJSONExpr(ctx, expr, options)
		if err != nil {
			return nil, err
		}
		if value != "" {
			values = append(values, value)
		}
	}
	return values, nil
}

func sqlJSONExprList(ctx context.Context, exprs []*plan.Expr, options *ExplainOptions) (string, error) {
	values, err := sqlJSONExprValues(ctx, exprs, options)
	if err != nil {
		return "", err
	}
	return strings.Join(values, ", "), nil
}

func sqlJSONExpr(ctx context.Context, expr *plan.Expr, options *ExplainOptions) (value string, err error) {
	if expr == nil {
		return "", nil
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			value = ""
			err = moerr.NewInvalidInputf(ctx, "failed to serialize plan expression: %v", recovered)
		}
	}()
	buf := bytes.NewBuffer(make([]byte, 0, 128))
	if err := describeExpr(ctx, expr, options, buf); err != nil {
		return "", err
	}
	return strings.TrimSpace(buf.String()), nil
}

func sqlJSONTableName(node *plan.Node) string {
	if node == nil {
		return ""
	}
	if node.TableDef != nil {
		if alias := sqlJSONTableAlias(node.TableDef); alias != "" {
			return alias
		}
	}
	if node.ObjRef != nil {
		if name := sqlJSONObjectRefName(node.ObjRef); name != "" {
			return name
		}
	}
	if node.TableDef != nil {
		if node.TableDef.DbName != "" && node.TableDef.Name != "" {
			return node.TableDef.DbName + "." + node.TableDef.Name
		}
		if node.TableDef.Name != "" {
			return node.TableDef.Name
		}
	}
	if node.DeleteCtx != nil {
		return sqlJSONTargetName(node.DeleteCtx.Ref, node.DeleteCtx.TableDef)
	}
	if node.InsertCtx != nil {
		return sqlJSONTargetName(node.InsertCtx.Ref, node.InsertCtx.TableDef)
	}
	return ""
}

func sqlJSONTargetName(ref *plan.ObjectRef, table *plan.TableDef) string {
	if name := sqlJSONObjectRefName(ref); name != "" {
		return name
	}
	if table == nil {
		return ""
	}
	if alias := sqlJSONTableAlias(table); alias != "" {
		return alias
	}
	if table.DbName != "" && table.Name != "" {
		return table.DbName + "." + table.Name
	}
	return table.Name
}

func sqlJSONTableAlias(table *plan.TableDef) string {
	if table == nil || len(table.Cols) == 0 {
		return ""
	}
	var alias string
	for _, col := range table.Cols {
		if col == nil || col.TblName == "" || col.OriginTblName == "" ||
			strings.EqualFold(col.TblName, col.OriginTblName) {
			continue
		}
		if alias == "" {
			alias = col.TblName
		} else if !strings.EqualFold(alias, col.TblName) {
			return ""
		}
	}
	return alias
}

func sqlJSONObjectRefName(ref *plan.ObjectRef) string {
	if ref == nil {
		return ""
	}
	qualifiedName := ref.SchemaName
	if qualifiedName == "" {
		qualifiedName = ref.DbName
	}
	if qualifiedName != "" && ref.ObjName != "" {
		return qualifiedName + "." + ref.ObjName
	}
	return ref.ObjName
}
