// Copyright 2025 Matrix Origin
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
	"strings"
	"testing"

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
)

func TestCostDescribeImpl_IncludesRowsizeWhenPositive(t *testing.T) {
	stats := &planpb.Stats{
		Cost:        10,
		Outcnt:      5,
		Selectivity: 0.5,
		Dop:         2,
		BlockNum:    3,
		Rowsize:     128.0,
	}
	impl := &CostDescribeImpl{Stats: stats}
	buf := new(bytes.Buffer)
	if err := impl.GetDescription(context.Background(), NewExplainDefaultOptions(), buf); err != nil {
		t.Fatalf("GetDescription error: %v", err)
	}
	got := buf.String()
	if !strings.Contains(got, "rowsize=128.00") {
		t.Fatalf("expected rowsize to be printed, got: %s", got)
	}
}

func TestCostDescribeImpl_OmitsRowsizeWhenZero(t *testing.T) {
	stats := &planpb.Stats{
		Cost:        1,
		Outcnt:      1,
		Selectivity: 1,
		Dop:         1,
		BlockNum:    1,
		Rowsize:     0,
	}
	impl := &CostDescribeImpl{Stats: stats}
	buf := new(bytes.Buffer)
	if err := impl.GetDescription(context.Background(), NewExplainDefaultOptions(), buf); err != nil {
		t.Fatalf("GetDescription error: %v", err)
	}
	got := buf.String()
	if strings.Contains(got, "rowsize=") {
		t.Fatalf("did not expect rowsize to be printed when zero, got: %s", got)
	}
}
