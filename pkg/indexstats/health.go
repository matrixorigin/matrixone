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

package indexstats

import (
	"context"
	"encoding/json"
	"io"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

const AnalyzeFunctionName = "mo_index_analyze"

type AnalyzeOptions struct {
	SampleCount  int     `json:"sample_count"`
	RecallTarget float64 `json:"recall_target"`
	Retrain      bool    `json:"retrain"`
}

func ParseAnalyzeOptions(ctx context.Context, raw string) (*AnalyzeOptions, error) {
	opts := &AnalyzeOptions{SampleCount: 100, RecallTarget: .95}
	if raw != "" {
		if !strings.HasPrefix(strings.TrimSpace(raw), "{") {
			return nil, moerr.NewInvalidInput(ctx, "index analysis options must be a JSON object")
		}
		dec := json.NewDecoder(strings.NewReader(raw))
		dec.DisallowUnknownFields()
		if err := dec.Decode(opts); err != nil {
			return nil, moerr.NewInvalidInputf(ctx, "invalid index analysis options: %v", err)
		}
		var extra any
		if err := dec.Decode(&extra); err != io.EOF {
			return nil, moerr.NewInvalidInput(ctx, "index analysis options must contain one JSON object")
		}
	}
	if opts.SampleCount < 1 || opts.SampleCount > 256 || !(opts.RecallTarget > 0 && opts.RecallTarget <= 1) {
		return nil, moerr.NewInvalidInput(ctx, "sample_count must be 1..256 and recall_target must be in (0,1]")
	}
	return opts, nil
}

type Suggestion struct {
	Action   string         `json:"action"`
	Reason   string         `json:"reason"`
	Params   map[string]any `json:"params,omitempty"`
	Variable string         `json:"variable,omitempty"`
	Value    any            `json:"value,omitempty"`
}

// AddReindex keeps one executable recommendation rather than conflicting copies.
func AddReindex(items []Suggestion, reason string, params map[string]any) []Suggestion {
	for _, s := range items {
		if s.Action == "REINDEX" {
			return items
		}
	}
	return append(items, Suggestion{Action: "REINDEX", Reason: reason, Params: params})
}
