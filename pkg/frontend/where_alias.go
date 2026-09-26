// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import "context"

func updateWhereAlias(_ context.Context, ses *Session, vars *SystemVariables, name string, value interface{}) error {
	old := vars.Get(name)
	if old == nil {
		old = int8(0) // The default is disabled, including in lightweight sessions.
	}
	vars.Set(name, value)
	if old == value {
		return nil
	}

	// Name resolution is part of a plan generation. Both text and prepared
	// plans must observe the new setting, including when it disables aliases.
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.releasePlanCache()
	for _, stmt := range ses.prepareStmts {
		stmt.needsRebuild = true
		stmt.compileNeedsRebuild = true
	}
	return nil
}
