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

package types

// ComparisonOp compares the left operand with the right operand.
// Values are also encoded by internal scalar endpoint functions.
type ComparisonOp uint8

const (
	ComparisonEqual ComparisonOp = iota
	ComparisonLess
	ComparisonLessEqual
	ComparisonGreater
	ComparisonGreaterEqual
)

func (op ComparisonOp) String() string {
	switch op {
	case ComparisonEqual:
		return "="
	case ComparisonLess:
		return "<"
	case ComparisonLessEqual:
		return "<="
	case ComparisonGreater:
		return ">"
	case ComparisonGreaterEqual:
		return ">="
	default:
		return ""
	}
}

func ParseComparisonOp(value string) (ComparisonOp, bool) {
	switch value {
	case "=":
		return ComparisonEqual, true
	case "<":
		return ComparisonLess, true
	case "<=":
		return ComparisonLessEqual, true
	case ">":
		return ComparisonGreater, true
	case ">=":
		return ComparisonGreaterEqual, true
	default:
		return 0, false
	}
}

func (op ComparisonOp) Reverse() ComparisonOp {
	switch op {
	case ComparisonLess:
		return ComparisonGreater
	case ComparisonLessEqual:
		return ComparisonGreaterEqual
	case ComparisonGreater:
		return ComparisonLess
	case ComparisonGreaterEqual:
		return ComparisonLessEqual
	default:
		return op
	}
}
