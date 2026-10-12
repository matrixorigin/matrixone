// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bytes"
	"fmt"
	"os"
)

// gogo preserves unknown scalar values. Install structural checks at the owner
// decoder, independent of the production admission fence. Re-running is safe.
func patchCollationMetadata(path string, messages ...string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	for _, message := range messages {
		start := bytes.Index(data, []byte("func (m *"+message+") Unmarshal(dAtA []byte) error {"))
		if start < 0 {
			return fmt.Errorf("generated %s.Unmarshal not found", message)
		}
		end := len(data)
		if next := bytes.Index(data[start+1:], []byte("\nfunc ")); next >= 0 {
			end = start + 1 + next
		}
		body := data[start:end]
		replacement := []byte("\treturn m.ValidateCollation()\n}")
		if bytes.Contains(body, replacement) {
			continue
		}
		old := []byte("\treturn nil\n}")
		position := bytes.LastIndex(body, old)
		if position < 0 {
			return fmt.Errorf("generated %s.Unmarshal return not found", message)
		}
		position += start
		updated := make([]byte, 0, len(data)+len(replacement)-len(old))
		updated = append(updated, data[:position]...)
		updated = append(updated, replacement...)
		data = append(updated, data[position+len(old):]...)
	}
	return os.WriteFile(path, data, 0o644)
}
