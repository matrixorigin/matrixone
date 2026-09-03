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

package plan

import (
	"encoding/hex"
	"strconv"
)

// MarshalText keeps the execution BSON opaque in every protobuf text
// representation. gogo's text marshaler uses this method for nested messages
// too, so a parent plan cannot disclose user query fields or values while the
// binary protobuf representation remains unchanged for execution.
func (m *MongoScan) MarshalText() ([]byte, error) {
	if m == nil {
		return []byte("<nil>"), nil
	}

	text := make([]byte, 0, 96)
	text = appendMongoScanDiagnosticField(text, "operation", mongoScanOperation(m.UserQueryKind))
	if digest, ok := mongoScanDiagnosticDigest(m.UserQueryDigest); ok {
		text = appendMongoScanDiagnosticField(text, "query_digest", digest)
	}
	if len(m.UserFilterBson) > 0 {
		text = appendMongoScanDiagnosticField(text, "user_filter_bson", "<redacted>")
	}
	if len(m.UserPipelineStageBson) > 0 {
		text = appendMongoScanDiagnosticField(text, "user_pipeline_stage_bson", "<redacted>")
	}
	return text, nil
}

func appendMongoScanDiagnosticField(text []byte, name, value string) []byte {
	if len(text) > 0 {
		text = append(text, ' ')
	}
	text = append(text, name...)
	text = append(text, ':')
	return strconv.AppendQuote(text, value)
}

func mongoScanOperation(kind int32) string {
	switch kind {
	case 0:
		return "find"
	case 1:
		return "find-filter"
	case 2:
		return "aggregate"
	default:
		return "invalid"
	}
}

func mongoScanDiagnosticDigest(digest string) (string, bool) {
	if len(digest) != 64 {
		return "", false
	}
	if _, err := hex.DecodeString(digest); err != nil {
		return "", false
	}
	return digest[:12], true
}
