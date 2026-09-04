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
//
// Portions derived from github.com/rashiq/mysql-digest; see ../LICENSE.

package internal

type reducer struct {
	store *tokenStore
}

// NewReducer creates a new reducer.
func NewReducer(store *tokenStore) *reducer {
	return &reducer{store: store}
}

// Absorbs unary +/- signs before numeric literals.
func (r *reducer) reduceUnarySign() {
	for {
		prev, last := r.store.peek2()
		if (last == '+' || last == '-') && startsExpression(prev) {
			r.store.pop(1)
		} else {
			break
		}
	}
}

// Checks for pattern: VALUE/VALUE_LIST ',' VALUE -> VALUE_LIST
func (r *reducer) reduceAfterValue() {
	first, comma, _ := r.store.peek3()
	if comma == ',' && isValueOrValueList(first) {
		r.store.pop(3)
		r.store.push(TOK_GENERIC_VALUE_LIST)
	}
	r.reduceAll()
}

func (r *reducer) reduceAll() {
	for {
		if r.reduceParenthesizedValue() {
			continue
		}
		if r.reduceRowList() {
			continue
		}
		if r.reduceInClause() {
			continue
		}
		break
	}
}

// reduceParenthesizedValue handles: '(' VALUE ')' -> ROW_VALUE
// - '(' TOK_GENERIC_VALUE ')' -> TOK_ROW_SINGLE_VALUE
// - '(' TOK_GENERIC_VALUE_LIST ')' -> TOK_ROW_MULTIPLE_VALUE
func (r *reducer) reduceParenthesizedValue() bool {
	if r.store.len() < 3 {
		return false
	}

	first, mid, last := r.store.peek3()

	if first != '(' || last != ')' {
		return false
	}

	switch mid {
	case TOK_GENERIC_VALUE:
		r.store.pop(3)
		r.store.push(TOK_ROW_SINGLE_VALUE)
		return true
	case TOK_GENERIC_VALUE_LIST:
		r.store.pop(3)
		r.store.push(TOK_ROW_MULTIPLE_VALUE)
		return true
	}
	return false
}

// reduceRowList handles: ROW ',' ROW -> ROW_LIST
// - Single-value rows: (?) , (?) -> (?), ...
// - Multi-value rows: (...) , (...) -> (...), ...
func (r *reducer) reduceRowList() bool {
	if r.store.len() < 3 {
		return false
	}

	first, comma, last := r.store.peek3()

	if comma != ',' {
		return false
	}

	if isSingleValueRow(first) && isSingleValueRow(last) {
		r.store.pop(3)
		r.store.push(TOK_ROW_SINGLE_VALUE_LIST)
		return true
	}

	if isMultiValueRow(first) && isMultiValueRow(last) {
		r.store.pop(3)
		r.store.push(TOK_ROW_MULTIPLE_VALUE_LIST)
		return true
	}

	return false
}

// reduceInClause handles: IN ROW -> IN (...)
func (r *reducer) reduceInClause() bool {
	if r.store.len() < 2 {
		return false
	}

	before, last := r.store.peek2()

	if before != IN_SYM {
		return false
	}

	if last == TOK_ROW_SINGLE_VALUE || last == TOK_ROW_MULTIPLE_VALUE {
		r.store.pop(2)
		r.store.push(TOK_IN_GENERIC_VALUE_EXPRESSION)
		return true
	}

	return false
}

func isValueOrValueList(tok int) bool {
	return tok == TOK_GENERIC_VALUE || tok == TOK_GENERIC_VALUE_LIST
}

func isSingleValueRow(tok int) bool {
	return tok == TOK_ROW_SINGLE_VALUE || tok == TOK_ROW_SINGLE_VALUE_LIST
}

func isMultiValueRow(tok int) bool {
	return tok == TOK_ROW_MULTIPLE_VALUE || tok == TOK_ROW_MULTIPLE_VALUE_LIST
}

func startsExpression(tokType int) bool {
	if tokType == 0 || tokType == TOK_UNUSED {
		return true
	}
	return TokenStartExpr(tokType)
}
