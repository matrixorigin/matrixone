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

package iceberg

import (
	"strconv"
	"strings"
	"time"
)

func lengthPrefixedKey(parts ...string) string {
	var out strings.Builder
	for _, part := range parts {
		out.WriteString(strconv.Itoa(len(part)))
		out.WriteByte(':')
		out.WriteString(part)
	}
	return out.String()
}

func quoteSQLString(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, "'", "''")
	return "'" + value + "'"
}

func nullOrSQLString(value string) string {
	if value == "" {
		return "null"
	}
	return quoteSQLString(value)
}

func trimNonEmpty(value string) string {
	return strings.TrimSpace(value)
}

func quoteUTCTimestamp(value time.Time) string {
	return quoteSQLString(value.UTC().Format("2006-01-02 15:04:05.000000"))
}

func timestampOrUTCNow(value time.Time) string {
	if value.IsZero() {
		return "utc_timestamp"
	}
	return quoteUTCTimestamp(value)
}
