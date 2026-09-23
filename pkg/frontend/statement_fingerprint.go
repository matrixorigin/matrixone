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

package frontend

import (
	"context"
	"crypto/sha256"
	"encoding/hex"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

const maxStatementFingerprintFormattedBytes = 4 << 20

// formatStatementFingerprint captures a best-effort fingerprint of a parsed
// statement before planning can rewrite its AST. The formatted SQL is bounded
// because this value is telemetry; an unavailable fingerprint must not reject
// an otherwise valid statement.
func formatStatementFingerprint(ctx context.Context, stmt tree.Statement) (fingerprint string, attempted bool) {
	if stmt == nil || ctx == nil {
		return "", true
	}
	if context.Cause(ctx) != nil {
		return "", true
	}
	attempted = true
	defer func() {
		if recover() != nil {
			fingerprint = ""
		}
	}()

	fmtCtx := tree.NewFmtCtx(
		dialect.MYSQL,
		tree.WithQuoteIdentifier(),
		tree.WithSingleQuoteString(),
		tree.WithCanonicalUserVariableNames(),
		tree.WithMaxOutputBytes(maxStatementFingerprintFormattedBytes),
	)
	stmt.Format(fmtCtx)
	if fmtCtx.OutputLimitExceeded() || fmtCtx.Len() == 0 || context.Cause(ctx) != nil {
		return "", true
	}
	sum := sha256.Sum256([]byte(fmtCtx.String()))
	return hex.EncodeToString(sum[:]), true
}
