// Copyright 2024 Matrix Origin
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

package plugin

import "strconv"

// ResolveVarFunc matches the session/system variable resolver signature used by
// proc.GetResolveVariableFunc, CompileContext.ResolveVariable and
// Metadata.ResolveVariableFunc.
type ResolveVarFunc = func(varName string, isSystem, isGlobal bool) (any, error)

// AlgoParamInt resolves an int build param (e.g. *_max_index_capacity,
// kmeans_max_iteration) with precedence:
//
//  1. flat — the value from the index's algo_params, present only when the
//     option was given in CREATE INDEX (ParamsFromTree writes it).
//  2. resolve(sessionVar) — the session/system variable, so `SET <var>=...`
//     still controls the build when the option is omitted from the DDL. The
//     variable carries its own system default, so this is the normal path.
//  3. def — used only when no resolver is available (internal SQL procs).
func AlgoParamInt(flat string, resolve ResolveVarFunc, sessionVar string, def int64) (int64, error) {
	if flat != "" {
		return strconv.ParseInt(flat, 10, 64)
	}
	if resolve != nil {
		if v, err := resolve(sessionVar, true, false); err == nil && v != nil {
			if i, ok := v.(int64); ok {
				return i, nil
			}
		}
	}
	return def, nil
}

// AlgoParamFloat is AlgoParamInt for float64 params (kmeans_train_percent).
func AlgoParamFloat(flat string, resolve ResolveVarFunc, sessionVar string, def float64) (float64, error) {
	if flat != "" {
		return strconv.ParseFloat(flat, 64)
	}
	if resolve != nil {
		if v, err := resolve(sessionVar, true, false); err == nil && v != nil {
			if f, ok := v.(float64); ok {
				return f, nil
			}
		}
	}
	return def, nil
}
