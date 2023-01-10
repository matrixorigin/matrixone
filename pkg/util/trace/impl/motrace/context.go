// Copyright 2022 Matrix Origin
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

package motrace

import "context"

type stmContextKeyType int

const currentStmKey stmContextKeyType = iota

func ContextWithStatement(parent context.Context, s *StatementInfo) context.Context {
	return context.WithValue(parent, currentStmKey, s)
}

func StatementFromContext(ctx context.Context) *StatementInfo {
	if ctx == nil {
		return nil
	} else if val := ctx.Value(currentStmKey); val == nil {
		return nil
	} else if stm, ok := val.(*StatementInfo); !ok {
		return nil
	} else {
		return stm
	}
}
