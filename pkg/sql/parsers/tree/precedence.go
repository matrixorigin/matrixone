// Copyright 2021 Matrix Origin
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

package tree

type Precedence int

const (
	Syntactic Precedence = iota
	P1
	P2
	P3
	P4
	P5
	P6
	P7
	P8
	P9
	P10
	P11
	P12
	P13
	P14
	P15
	P16
	P17
)

func precedenceFor(in Expr) Precedence {
	switch node := in.(type) {
	case *OrExpr:
		return P16
	case *XorExpr:
		return P15
	case *AndExpr:
		return P14
	case *NotExpr:
		return P13
	case *RangeCond:
		return P12
	case *ComparisonExpr:
		switch node.Op {
		case EQUAL, NOT_EQUAL, GREAT_THAN, GREAT_THAN_EQUAL, LESS_THAN, LESS_THAN_EQUAL, LIKE, IN, REG_MATCH:
			return P11
		}
	case *IsNullExpr, *IsNotNullExpr:
		return P11
	case *BinaryExpr:
		switch node.Op {
		case BIT_OR:
			return P10
		case BIT_AND:
			return P9
		case LEFT_SHIFT, RIGHT_SHIFT:
			return P8
		case PLUS, MINUS:
			return P7
		case DIV, MULTI, MOD, INTEGER_DIV:
			return P6
		case BIT_XOR:
			return P5
		}
	case *UnaryExpr:
		switch node.Op {
		case UNARY_PLUS, UNARY_MINUS:
			return P4
		// TODO:
		case UNARY_TILDE:
			return P3
		// TODO:
		case UNARY_MARK:
			return P2
		}
	case *IntervalExpr:
		return P1
	}

	return Syntactic
}
