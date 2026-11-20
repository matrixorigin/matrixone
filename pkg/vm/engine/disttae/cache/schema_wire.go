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

package cache

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

// schemaConstraint owns the original constraint encoding. The effective planner
// properties are not a reversible representation of this stream.
type schemaConstraint struct{ raw []byte }

func newSchemaConstraint(raw []byte) schemaConstraint {
	return schemaConstraint{raw: bytes.Clone(raw)}
}

// The constraint decoder constructs private values and copies protobuf byte
// fields, leaving the published wire bytes untouched.
func (c schemaConstraint) decode() (*engine.ConstraintDef, error) {
	d := &engine.ConstraintDef{}
	if err := d.UnmarshalBinary(c.raw); err != nil {
		return nil, err
	}
	return d, nil
}
