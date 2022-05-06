package explain

import (
	"fmt"
	// "container/list"
	// "fmt"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

var _ ExplainQuery = &ExplainQueryImpl{}

type ExplainQueryImpl struct {
	QueryPlan *plan.Query
}

func NewExplainQueryImpl(query *plan.Query) *ExplainQueryImpl {
	return &ExplainQueryImpl{
		QueryPlan: query,
	}
}

func (e *ExplainQueryImpl) ExplainPlan(buffer *ExplainDataBuffer, options *ExplainOptions) {
	var Nodes []*plan.Node = e.QueryPlan.Nodes
	for index, rootNodeId := range e.QueryPlan.Steps {
		fmt.Printf("-----------------------------Printing the %v-th plan tree -----------------------\n", index)
		settings := FormatSettings{
			buffer:      buffer,
			offset:      0,
			indent:      4,
			indent_char: ' ',
			level:       0,
		}

		stack := NewStack()
		stack.Push(&Frame{
			node:                 Nodes[rootNodeId],
			is_description_print: false,
			next_child:           0,
		})

		for !stack.Empty() {
			frame := stack.Top()
			if !frame.is_description_print {
				settings.offset = (stack.Size()) * settings.indent
				explainStep(frame.node, &settings, options)
				frame.is_description_print = true
			}

			if frame.next_child < len(frame.node.Children) {
				childId := frame.node.Children[frame.next_child]
				stack.Push(&Frame{
					node:                 Nodes[childId],
					is_description_print: false,
					next_child:           0,
				})
				frame.next_child++
			} else {
				stack.Pop()
			}
		}
		//Separate the different plan trees with blank lines
		settings.buffer.PushLine(0, " ", true, false)
	}
}

func (e *ExplainQueryImpl) ExplainAnalyze(buffer *ExplainDataBuffer, options *ExplainOptions) {
	//TODO implement me
	panic("implement me")
}

func explainStep(step *plan.Node, settings *FormatSettings, options *ExplainOptions) {
	nodedescImpl := NewNodeDescriptionImpl(step)

	if options.Format == EXPLAIN_FORMAT_TEXT {
		//ExplainIndentText(settings, options)
		basicNodeInfo := nodedescImpl.GetNodeBasicInfo(options)
		if settings.level == 0 {
			settings.buffer.PushLine(settings.offset, basicNodeInfo, true, true)
			settings.level++
		} else {
			settings.buffer.PushLine(settings.offset, basicNodeInfo, false, true)
			settings.level++
		}

		if options.Verbose {
			projecrtInfo := nodedescImpl.GetProjectListInfo(options)
			settings.buffer.PushLine(settings.offset, projecrtInfo, false, false)
		}

		extraInfo := nodedescImpl.GetExtraInfo(options)
		for _, line := range extraInfo {
			settings.buffer.PushLine(settings.offset, line, false, false)
		}
	} else if options.Format == EXPLAIN_FORMAT_JSON {

	} else if options.Format == EXPLAIN_FORMAT_DOT {

	}
}
