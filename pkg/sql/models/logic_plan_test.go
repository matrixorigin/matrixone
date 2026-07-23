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

package models

import (
	"context"
	"testing"
)

func TestStatisticsGlobalResourceKeepsDiagnosticMemory(t *testing.T) {
	graph := NewGraphData(1)
	graph.Nodes = append(graph.Nodes, Node{
		Statistics: Statistics{
			Memory: []StatisticValue{{Name: MemorySize, Value: 12345, Unit: "byte"}},
		},
	})

	if err := graph.StatisticsGlobalResource(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(graph.Global.Statistics.Memory) != 1 || graph.Global.Statistics.Memory[0].Value != 12345 {
		t.Fatalf("operator memory diagnostic missing from global logical explain: %+v", graph.Global.Statistics.Memory)
	}
}
