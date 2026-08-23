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

package main

import (
	"bytes"
	"fmt"
	"os"
)

func main() {
	if len(os.Args) != 2 {
		panic("usage: plan_string_literal_form <plan.pb.go>")
	}
	path := os.Args[1]
	data, err := os.ReadFile(path)
	if err != nil {
		panic(err)
	}
	start := bytes.Index(data, []byte("func (m *Expr) Unmarshal(dAtA []byte) error {"))
	next := bytes.Index(data, []byte("func (m *FoldVal) Unmarshal(dAtA []byte) error {"))
	if start < 0 || next <= start {
		panic("generated Expr.Unmarshal boundary not found")
	}
	body := data[start:next]
	old := []byte("\treturn nil\n}\n")
	replacement := []byte("\treturn m.validateOwnStringLiteralForm()\n}\n")
	if bytes.Contains(body, replacement) {
		return
	}
	position := bytes.LastIndex(body, old)
	if position < 0 {
		panic("generated Expr.Unmarshal return not found")
	}
	absolute := start + position
	patched := make([]byte, 0, len(data)-len(old)+len(replacement))
	patched = append(patched, data[:absolute]...)
	patched = append(patched, replacement...)
	patched = append(patched, data[absolute+len(old):]...)
	if err := os.WriteFile(path, patched, 0o644); err != nil {
		panic(fmt.Errorf("write generated plan: %w", err))
	}
}
