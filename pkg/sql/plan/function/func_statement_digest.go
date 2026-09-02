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

package function

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func StatementDigestText(
	parameters []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	selectList *FunctionSelectList,
) error {
	ctx := context.Background()
	if proc != nil && proc.Ctx != nil {
		ctx = proc.Ctx
	}
	sqlMode := statementDigestSQLMode(proc)
	maxDigestLength := statementDigestMaxLength(proc)
	discloseParseError := parameters[0].IsConst() &&
		(proc == nil || proc.Base == nil || proc.GetPrepareParams() == nil)

	return opUnaryBytesToBytesWithErrorCheck(
		parameters, result, proc, length,
		func(sql []byte) ([]byte, error) {
			normalized, err := mysql.NormalizeStatementDigest(ctx, string(sql), sqlMode, maxDigestLength)
			if err != nil {
				if discloseParseError {
					return nil, moerr.NewParseErrorInDigestFunction(ctx, err.Error())
				}
				return nil, moerr.NewUndisclosedParseErrorInDigestFunction(ctx)
			}
			return []byte(normalized), nil
		},
		selectList,
	)
}

func statementDigestMaxLength(proc *process.Process) int {
	const (
		defaultMaxDigestLength = 1024
		maximumMaxDigestLength = 1 << 20
	)
	if proc == nil || proc.Base == nil || proc.GetResolveVariableFunc() == nil {
		return defaultMaxDigestLength
	}
	value, err := proc.GetResolveVariableFunc()("max_digest_length", true, true)
	if err != nil {
		return defaultMaxDigestLength
	}
	var length int
	switch n := value.(type) {
	case int64:
		length = int(n)
	case uint64:
		if n > maximumMaxDigestLength {
			return defaultMaxDigestLength
		}
		length = int(n)
	case int:
		length = n
	default:
		return defaultMaxDigestLength
	}
	if length < 0 || length > maximumMaxDigestLength {
		return defaultMaxDigestLength
	}
	return length
}

func statementDigestSQLMode(proc *process.Process) string {
	if proc == nil || proc.Base == nil {
		return ""
	}
	mode := proc.GetSessionInfo().SqlMode
	if resolver := proc.GetResolveVariableFunc(); resolver != nil {
		if value, err := resolver("sql_mode", true, false); err == nil {
			if sessionMode, ok := value.(string); ok {
				mode = sessionMode
			}
		}
	}
	if mode == process.EmptySqlModeSentinel {
		return ""
	}
	return mode
}
