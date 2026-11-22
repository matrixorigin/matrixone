// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package vm

// SelectionConsumer is implemented by operators that read a selected batch
// (batch.Batch.HasSelection) on some child edge. Undeclared edges, the
// pipeline root, codecs and remote boundaries always receive compacted rows.
type SelectionConsumer interface {
	AcceptsSelection(child int) bool
}

// SelectionProducer is implemented by operators that can publish a selected
// batch instead of compacting their output. SetSelectionOutput is called after
// the operator's parent is prepared; until then (and at the root) the
// producer must compact.
type SelectionProducer interface {
	SetSelectionOutput(allowed bool)
}

// configureSelectionChildren tells op's producer children whether op reads
// their selected output.
func configureSelectionChildren(op Operator) {
	consumer, accepts := op.(SelectionConsumer)
	for i, child := range op.GetOperatorBase().Children {
		if producer, ok := child.(SelectionProducer); ok {
			producer.SetSelectionOutput(accepts && consumer.AcceptsSelection(i))
		}
	}
}

// ConfigureSelectionEdges configures every edge of an operator tree whose
// operators were prepared individually rather than through Prepare. The root
// keeps compacted output.
func ConfigureSelectionEdges(root Operator) {
	if root == nil {
		return
	}
	if producer, ok := root.(SelectionProducer); ok {
		producer.SetSelectionOutput(false)
	}
	var walk func(Operator)
	walk = func(op Operator) {
		configureSelectionChildren(op)
		for _, child := range op.GetOperatorBase().Children {
			walk(child)
		}
	}
	walk(root)
}
