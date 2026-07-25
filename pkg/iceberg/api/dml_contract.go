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

package api

import (
	"bytes"
	"encoding/json"
	"strings"
)

const (
	// DMLDeletePlanExtraOptions, DMLUpdatePlanExtraOptions,
	// DMLMergePlanExtraOptions, and DMLOverwritePlanExtraOptions mark a
	// plan.Node_INSERT as an Iceberg
	// row-level DML sink rather than an append INSERT. They are carried in the
	// existing Node.extra_options field to avoid widening the public plan proto
	// while the DML operator contract is still being staged.
	DMLDeletePlanExtraOptions    = "iceberg_dml_delete"
	DMLUpdatePlanExtraOptions    = "iceberg_dml_update"
	DMLMergePlanExtraOptions     = "iceberg_dml_merge"
	DMLOverwritePlanExtraOptions = "iceberg_dml_overwrite"

	DMLPlanExtraOptionsEnvelopePrefix = "MO_ICEBERG_DML:"

	// DMLDataFilePathColumnName and DMLRowOrdinalColumnName are internal
	// scan-only columns used by Iceberg row-level DML collectors. The scan
	// reader materializes them from file/row side-channel metadata when a DML
	// plan explicitly projects them.
	DMLDataFilePathColumnName   = "__mo_iceberg_data_file_path"
	DMLRowOrdinalColumnName     = "__mo_iceberg_row_ordinal"
	DMLMergeActionColumnName    = "__mo_iceberg_merge_action"
	DMLMergeActionDelete        = "delete"
	DMLMergeActionUpdate        = "update"
	DMLMergeActionInsert        = "insert"
	DMLMergeActionNoop          = "noop"
	DMLMergeActionMatchedDelete = "matched_delete"
	DMLMergeActionMatchedUpdate = "matched_update"
	DMLMergeActionNotMatched    = "not_matched_insert"
)

type DMLPlanExtraOptions struct {
	Kind               string         `json:"kind"`
	OverwriteScope     string         `json:"overwrite_scope,omitempty"`
	OverwritePartition map[string]any `json:"overwrite_partition,omitempty"`
}

func EncodeDMLOverwritePartitionPlanExtraOptions(partition map[string]any) (string, error) {
	if len(partition) == 0 {
		return DMLOverwritePlanExtraOptions, nil
	}
	payload := DMLPlanExtraOptions{
		Kind:               DMLOverwritePlanExtraOptions,
		OverwriteScope:     "partition",
		OverwritePartition: partition,
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	return DMLPlanExtraOptionsEnvelopePrefix + string(b), nil
}

func DecodeDMLPlanExtraOptions(value string) (DMLPlanExtraOptions, error) {
	value = strings.TrimSpace(value)
	switch value {
	case "", DMLDeletePlanExtraOptions, DMLUpdatePlanExtraOptions, DMLMergePlanExtraOptions, DMLOverwritePlanExtraOptions:
		return DMLPlanExtraOptions{Kind: value}, nil
	}
	if !strings.HasPrefix(value, DMLPlanExtraOptionsEnvelopePrefix) {
		return DMLPlanExtraOptions{Kind: value}, nil
	}
	dec := json.NewDecoder(bytes.NewBufferString(strings.TrimPrefix(value, DMLPlanExtraOptionsEnvelopePrefix)))
	dec.UseNumber()
	var opts DMLPlanExtraOptions
	if err := dec.Decode(&opts); err != nil {
		return DMLPlanExtraOptions{}, err
	}
	opts.OverwritePartition = normalizeDMLPlanExtraOptionsMap(opts.OverwritePartition)
	return opts, nil
}

func normalizeDMLPlanExtraOptionsMap(values map[string]any) map[string]any {
	if len(values) == 0 {
		return values
	}
	out := make(map[string]any, len(values))
	for k, v := range values {
		out[k] = normalizeDMLPlanExtraOptionsValue(v)
	}
	return out
}

func normalizeDMLPlanExtraOptionsValue(value any) any {
	switch v := value.(type) {
	case json.Number:
		if i, err := v.Int64(); err == nil {
			return i
		}
		if f, err := v.Float64(); err == nil {
			return f
		}
		return v.String()
	case map[string]any:
		return normalizeDMLPlanExtraOptionsMap(v)
	case []any:
		out := make([]any, len(v))
		for i := range v {
			out[i] = normalizeDMLPlanExtraOptionsValue(v[i])
		}
		return out
	default:
		return value
	}
}
