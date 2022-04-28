package explain

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan2"
)

type ExplainQuery interface {
	explainPlan(buffer *ExplainDataBuffer, options ExplainOptions)
	explainAnalyze(buffer *ExplainDataBuffer, options ExplainOptions)
	explainPipeline(buffer *ExplainDataBuffer, options ExplainOptions)
}

type ExplainQueryImpl struct {
	QueryPlan *plan2.Query
}

func NewExplainQueryImpl(query *plan2.Query) *ExplainQueryImpl {
	return &ExplainQueryImpl{
		QueryPlan: query,
	}
}

var _ ExplainQuery = &ExplainQueryImpl{}

func (e *ExplainQueryImpl) explainPlan(buffer *ExplainDataBuffer, options ExplainOptions) {
	//TODO implement me
	panic("implement me")
}

func (e *ExplainQueryImpl) explainAnalyze(buffer *ExplainDataBuffer, options ExplainOptions) {
	//TODO implement me
	panic("implement me")
}

func (e *ExplainQueryImpl) explainPipeline(buffer *ExplainDataBuffer, options ExplainOptions) {
	//TODO implement me
	panic("implement me")
}

//-------------------------------------------------------------------------------------------------------

type NodeDescribe interface {
	GetNodeBasicInfo() string
	GetVerboseInfo() string
	GetExtraInfo() string
	GetProjectListInfo() string
	GetJoinConditionInfo() string
	GetWhereConditionInfo() string
	GetGroupByInfo() string
}

var _ NodeDescribe = &NodeDescribeImpl{}

type NodeDescribeImpl struct {
	PlanNode *plan2.Node
}

func NewNodeDescriptionImpl(node *plan2.Node) *NodeDescribeImpl {
	return &NodeDescribeImpl{
		PlanNode: node,
	}
}

func (ndesc NodeDescribeImpl) GetNodeBasicInfo() string {
	//TODO implement me
	panic("implement me")
}

func (ndesc NodeDescribeImpl) GetVerboseInfo() string {
	//TODO implement me
	panic("implement me")
}

func (ndesc NodeDescribeImpl) GetExtraInfo() string {
	//TODO implement me
	panic("implement me")
}

func (ndesc NodeDescribeImpl) GetProjectListInfo() string {
	//TODO implement me
	panic("implement me")
}

func (ndesc NodeDescribeImpl) GetJoinConditionInfo() string {
	//TODO implement me
	panic("implement me")
}

func (ndesc NodeDescribeImpl) GetWhereConditionInfo() string {
	//TODO implement me
	panic("implement me")
}

func (ndesc NodeDescribeImpl) GetGroupByInfo() string {
	//TODO implement me
	panic("implement me")
}

//---------------------------------------------------------------------------------------------

type NodeElemDescribe interface {
	GetDescription() string
}

var _ NodeElemDescribe = &CostDescribeImpl{}
var _ NodeElemDescribe = &ExprListDescribeImpl{}
var _ NodeElemDescribe = &OrderByDescribeImpl{}
var _ NodeElemDescribe = &WinSpecDescribeImpl{}
var _ NodeElemDescribe = &TableDefDescribeImpl{}
var _ NodeElemDescribe = &ObjRefDescribeImpl{}
var _ NodeElemDescribe = &RowsetDataDescribeImpl{}

type CostDescribeImpl struct {
	Cost *plan2.Cost
}

func (c CostDescribeImpl) GetDescription() string {
	//TODO implement me
	panic("implement me")
}

type ExprListDescribeImpl struct {
	ExprList []*plan2.Expr // ProjectList,OnList,WhereList,GroupBy,GroupingSet and so on
}

func (e ExprListDescribeImpl) GetDescription() string {
	//TODO implement me
	panic("implement me")
}

type OrderByDescribeImpl struct {
	OrderBy *plan.OrderBySpec
}

func (o OrderByDescribeImpl) GetDescription() string {
	//TODO implement me
	panic("implement me")
}

type WinSpecDescribeImpl struct {
	WinSpec *plan.WindowSpec
}

func (w WinSpecDescribeImpl) GetDescription() string {
	//TODO implement me
	panic("implement me")
}

type TableDefDescribeImpl struct {
	TableDef *plan2.TableDef
}

func (t TableDefDescribeImpl) GetDescription() string {
	//TODO implement me
	panic("implement me")
}

type ObjRefDescribeImpl struct {
	ObjRef *plan2.ObjectRef
}

func (o ObjRefDescribeImpl) GetDescription() string {
	//TODO implement me
	panic("implement me")
}

type RowsetDataDescribeImpl struct {
	RowsetData *plan2.RowsetData
}

func (r RowsetDataDescribeImpl) GetDescription() string {
	//TODO implement me
	panic("implement me")
}

//-------------------------------------------------------------------------------------

type FormatSettings struct {
	buffer      *ExplainDataBuffer
	offset      int32
	indent      int32
	indent_char byte
}

func NewFormatSettings() *FormatSettings {
	return &FormatSettings{
		buffer:      &ExplainDataBuffer{},
		offset:      0,
		indent:      2,
		indent_char: ' ',
	}
}

type ExplainDataBuffer struct {
	Start          int32
	End            int32
	CurrentLine    int32
	RowSize        int32
	LineWidthLimit int32
	Lines          []string
	NodeSize       int32
}

func NewExplainDataBuffer(size int32) *ExplainDataBuffer {
	return &ExplainDataBuffer{}
}

// Generates a string describing a ExplainDataBuffer.
func (buf *ExplainDataBuffer) ToString() string {
	return fmt.Sprintf("ExplainDataBuffer{start: %d, end: %d, lines: %s, rowSize: %d}", buf.Start, buf.End, buf.Lines, buf.RowSize)
}

func (buf *ExplainDataBuffer) AppendCurrentLine(temp string) {
	//TODO
}

func (buf *ExplainDataBuffer) PushLine(line string) {
	//TODO
}

func (buf *ExplainDataBuffer) IsFull() bool {
	return false
	//TODO
}

func (buf *ExplainDataBuffer) Empty() bool {
	return false
	//TODO
}

type ExplainFormat int32

const (
	EXPLAIN_FORMAT_TEXT ExplainFormat = 0
	EXPLAIN_FORMAT_XML  ExplainFormat = 1
	EXPLAIN_FORMAT_JSON ExplainFormat = 2
	EXPLAIN_FORMAT_DOT  ExplainFormat = 3
)

type ExplainOptions struct {
	Verbose bool
	Format  ExplainFormat
}

func NewExplainPlanOptions() *ExplainOptions {
	return nil
}

type QueryPlanSetting struct {
	Name             string
	Optimize         bool
	JSON             bool
	DOT              bool
	QueryPlanOptions ExplainOptions
}
