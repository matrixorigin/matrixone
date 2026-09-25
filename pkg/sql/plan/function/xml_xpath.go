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

package function

import (
	"context"
	"math"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

const xmlXPathLimit = 16 << 10

type xmlPredicate struct {
	position          int // -1 is last(); zero is a name test.
	posCompare        *xmlPositionCompare
	attribute, exists bool
	name, value       string
}
type xmlPositionOperand struct {
	kind  byte // p position(), l last(), n numeric literal
	value float64
}
type xmlPositionCompare struct {
	left, right xmlPositionOperand
	op          string
}
type xmlStep struct {
	name       string
	axis       byte // c child, a attribute, s self, p parent
	descendant bool
	text       bool
	predicates []xmlPredicate
}
type xmlPath struct {
	steps []xmlStep
}
type xmlXPath struct {
	paths []xmlPath // node-set selection, or the left aggregate's input
	left  *xmlNumericOperand
	op    string
	right *xmlNumericOperand
}
type xmlNumericOperand struct {
	kind    byte // c count(), s sum(), n integer literal
	paths   []xmlPath
	value   float64
	integer int64
}
type xmlPathParser struct {
	ctx          context.Context
	s            string
	pos, records int
}

func (p *xmlPathParser) failure() error {
	return moerr.NewNotSupported(p.ctx, "XPATH syntax error or unsupported XPath feature")
}
func (p *xmlPathParser) space() {
	for p.pos < len(p.s) && xmlSpace(p.s[p.pos]) {
		p.pos++
	}
}
func (p *xmlPathParser) take(s string) bool {
	p.space()
	if strings.HasPrefix(p.s[p.pos:], s) {
		p.pos += len(s)
		return true
	}
	return false
}
func (p *xmlPathParser) name() string {
	p.space()
	start := p.pos
	p.pos = xmlNameEnd(p.s, p.pos)
	name := p.s[start:p.pos]
	if !xmlXPathQName(name) {
		return ""
	}
	return name
}

func xmlXPathQName(name string) bool {
	colon := strings.IndexByte(name, ':')
	if colon < 0 {
		return xmlXPathQNamePart(name)
	}
	if colon == 0 || colon+1 == len(name) || strings.IndexByte(name[colon+1:], ':') >= 0 {
		return false
	}
	return xmlXPathQNamePart(name[:colon]) && xmlXPathQNamePart(name[colon+1:])
}

func xmlXPathQNamePart(name string) bool {
	if name == "" {
		return false
	}
	r, width := utf8.DecodeRuneInString(name)
	if !(unicode.IsLetter(r) || r == '_') {
		return false
	}
	for pos := width; pos < len(name); {
		r, width = utf8.DecodeRuneInString(name[pos:])
		if !(unicode.IsLetter(r) || r == '_' || unicode.IsDigit(r) || r == '-' || r == '.' || unicode.IsMark(r)) {
			return false
		}
		pos += width
	}
	return true
}
func (p *xmlPathParser) number() (int, bool) {
	p.space()
	start := p.pos
	for p.pos < len(p.s) && p.s[p.pos] >= '0' && p.s[p.pos] <= '9' {
		p.pos++
	}
	n, err := strconv.Atoi(p.s[start:p.pos])
	return n, err == nil && n > 0
}
func (p *xmlPathParser) decimal() (float64, bool) {
	for p.pos < len(p.s) && (xmlSpace(p.s[p.pos]) || p.s[p.pos] == '\v' || p.s[p.pos] == '\f') {
		p.pos++
	}
	start := p.pos
	if p.pos < len(p.s) && (p.s[p.pos] == '+' || p.s[p.pos] == '-') {
		p.pos++
	}
	digits := 0
	for p.pos < len(p.s) && p.s[p.pos] >= '0' && p.s[p.pos] <= '9' {
		p.pos++
		digits++
	}
	if p.pos < len(p.s) && p.s[p.pos] == '.' {
		p.pos++
		for p.pos < len(p.s) && p.s[p.pos] >= '0' && p.s[p.pos] <= '9' {
			p.pos++
			digits++
		}
	}
	if digits == 0 {
		p.pos = start
		return 0, false
	}
	if p.pos < len(p.s) && (p.s[p.pos] == 'e' || p.s[p.pos] == 'E') {
		exponent := p.pos
		p.pos++
		if p.pos < len(p.s) && (p.s[p.pos] == '+' || p.s[p.pos] == '-') {
			p.pos++
		}
		first := p.pos
		for p.pos < len(p.s) && p.s[p.pos] >= '0' && p.s[p.pos] <= '9' {
			p.pos++
		}
		if first == p.pos {
			p.pos = exponent
		}
	}
	v, err := strconv.ParseFloat(p.s[start:p.pos], 64)
	return v, err == nil && !math.IsInf(v, 0) && !math.IsNaN(v)
}

// The bounded XPath subset admits only signed 64-bit integer literals.
// sum() text conversion is separate from XPath tokenization.
func (p *xmlPathParser) literal() (int64, bool) {
	p.space()
	start := p.pos
	if p.pos < len(p.s) && p.s[p.pos] == '-' {
		p.pos++
	}
	first := p.pos
	for p.pos < len(p.s) && p.s[p.pos] >= '0' && p.s[p.pos] <= '9' {
		p.pos++
	}
	if first == p.pos {
		p.pos = start
		return 0, false
	}
	value, err := strconv.ParseInt(p.s[start:p.pos], 10, 64)
	if err != nil || value == math.MinInt64 {
		p.pos = start
		return 0, false
	}
	return value, true
}
func (p *xmlPathParser) comparison() string {
	for _, op := range []string{"!=", "<=", ">=", "=", "<", ">"} {
		if p.take(op) {
			return op
		}
	}
	return ""
}
func (p *xmlPathParser) positionOperand() (xmlPositionOperand, bool) {
	if p.take("position()") {
		return xmlPositionOperand{kind: 'p'}, true
	}
	if p.take("last()") {
		return xmlPositionOperand{kind: 'l'}, true
	}
	if value, ok := p.literal(); ok {
		return xmlPositionOperand{kind: 'n', value: float64(value)}, true
	}
	return xmlPositionOperand{}, false
}
func (p *xmlPathParser) record() error {
	p.records++
	if p.records > 1024 {
		return moerr.NewInvalidInput(p.ctx, "XPath AST limit exceeded")
	}
	return p.ctx.Err()
}
func (p *xmlPathParser) predicate() (xmlPredicate, error) {
	var out xmlPredicate
	if err := p.record(); err != nil {
		return out, err
	}
	p.space()
	if p.take("last()") {
		if op := p.comparison(); op != "" {
			right, ok := p.positionOperand()
			if !ok {
				return out, p.failure()
			}
			out.posCompare = &xmlPositionCompare{xmlPositionOperand{kind: 'l'}, right, op}
		} else {
			out.position = -1
		}
	} else if p.take("position()") {
		op := p.comparison()
		if op == "" {
			return out, p.failure()
		}
		right, ok := p.positionOperand()
		if !ok {
			return out, p.failure()
		}
		out.posCompare = &xmlPositionCompare{xmlPositionOperand{kind: 'p'}, right, op}
	} else if p.pos < len(p.s) && p.s[p.pos] >= '0' && p.s[p.pos] <= '9' {
		n, ok := p.number()
		if !ok {
			return out, p.failure()
		}
		out.position = n
	} else {
		out.attribute = p.take("@")
		out.name = p.name()
		if out.name == "" {
			return out, p.failure()
		}
		if p.take("=") {
			p.space()
			if p.pos >= len(p.s) || (p.s[p.pos] != '\'' && p.s[p.pos] != '"') {
				return out, p.failure()
			}
			quote := p.s[p.pos]
			p.pos++
			start := p.pos
			for p.pos < len(p.s) && p.s[p.pos] != quote {
				p.pos++
			}
			if p.pos == len(p.s) {
				return out, p.failure()
			}
			out.value = p.s[start:p.pos]
			p.pos++
		} else if out.attribute {
			out.exists = true
		} else {
			return out, p.failure()
		}
	}
	if !p.take("]") {
		return out, p.failure()
	}
	return out, nil
}
func (p *xmlPathParser) path() (xmlPath, error) {
	var out xmlPath
	if err := p.record(); err != nil {
		return out, err
	}
	descendant := false
	if p.take("/") {
		descendant = p.take("/")
		p.space()
		if p.pos == len(p.s) || p.s[p.pos] == '|' || p.s[p.pos] == ')' {
			if descendant {
				return out, p.failure()
			}
			return out, nil
		}
	}
	for {
		if err := p.record(); err != nil {
			return out, err
		}
		if len(out.steps) >= 128 {
			return out, moerr.NewInvalidInput(p.ctx, "XPath step limit exceeded")
		}
		s := xmlStep{axis: 'c', descendant: descendant}
		if p.take("text()") {
			// text() is a terminal child-text selector. Its descendant bit is
			// retained so /a/text() and /a//text() select different records.
			s.text = true
			out.steps = append(out.steps, s)
			return out, nil
		}
		switch {
		case p.take(".."):
			s.axis = 'p'
		case p.take("."):
			s.axis = 's'
		default:
			if p.take("@") {
				s.axis = 'a'
			}
			if p.take("*") {
				s.name = "*"
			} else {
				s.name = p.name()
			}
			if s.name == "" {
				return out, p.failure()
			}
		}
		for p.take("[") {
			pred, err := p.predicate()
			if err != nil {
				return out, err
			}
			s.predicates = append(s.predicates, pred)
		}
		out.steps = append(out.steps, s)
		if !p.take("/") {
			break
		}
		descendant = p.take("/")
	}
	return out, nil
}

func (p *xmlPathParser) paths() ([]xmlPath, error) {
	var paths []xmlPath
	for {
		path, err := p.path()
		if err != nil {
			return nil, err
		}
		paths = append(paths, path)
		if !p.take("|") {
			break
		}
	}
	return paths, nil
}
func (p *xmlPathParser) numericOperand() (*xmlNumericOperand, bool, error) {
	start := p.pos
	for _, fn := range []struct {
		name string
		kind byte
	}{{"count", 'c'}, {"sum", 's'}} {
		p.pos = start
		if !p.take(fn.name) || p.pos >= len(p.s) || p.s[p.pos] != '(' {
			continue
		}
		p.pos++
		if err := p.record(); err != nil {
			return nil, true, err
		}
		paths, err := p.paths()
		if err != nil {
			return nil, true, err
		}
		if !p.take(")") {
			return nil, true, p.failure()
		}
		for _, path := range paths {
			if path.terminalText() {
				return nil, true, p.failure()
			}
		}
		return &xmlNumericOperand{kind: fn.kind, paths: paths}, true, nil
	}
	p.pos = start
	if value, ok := p.literal(); ok {
		if err := p.record(); err != nil {
			return nil, true, err
		}
		return &xmlNumericOperand{kind: 'n', value: float64(value), integer: value}, true, nil
	}
	p.pos = start
	return nil, false, nil
}
func compileXMLXPath(ctx context.Context, s string) (*xmlXPath, error) {
	if len(s) > xmlXPathLimit {
		return nil, moerr.NewInvalidInput(ctx, "XPath exceeds 16 KiB")
	}
	p := xmlPathParser{ctx: ctx, s: s}
	out := &xmlXPath{}
	left, numeric, err := p.numericOperand()
	if err != nil {
		return nil, err
	}
	if numeric {
		out.left = left
		out.paths = left.paths
		out.op = p.comparison()
		if out.op != "" {
			out.right, numeric, err = p.numericOperand()
			if err != nil {
				return nil, err
			}
			if !numeric {
				return nil, p.failure()
			}
		}
	} else {
		out.paths, err = p.paths()
		if err != nil {
			return nil, err
		}
	}
	p.space()
	if p.pos != len(s) {
		return nil, p.failure()
	}
	return out, nil
}

func (p xmlPath) terminalText() bool {
	return len(p.steps) > 0 && p.steps[len(p.steps)-1].text
}

func (d *xmlFragment) appendCandidate(dst []int, id int) ([]int, error) {
	if err := d.budget.spend(1, 32); err != nil {
		return nil, err
	}
	return append(dst, id), nil
}
func (d *xmlFragment) predicateLiteralMatches(id int, value string) (bool, error) {
	n := d.nodes[id]
	if n.kind == xmlAttribute {
		if err := d.budget.spend(1+(len(n.value)+len(value))/64, 0); err != nil {
			return false, err
		}
		return strings.EqualFold(n.value, value), nil
	}
	// Predicate comparison is separate from extraction formatting. MySQL's
	// ExtractValue-compatible subset compares each direct text record, rather
	// than joining records separated by child elements with a space.
	for c := n.first; c >= 0; c = d.nodes[c].next {
		child := d.nodes[c]
		if err := d.budget.spend(1+len(child.value)/64, 0); err != nil {
			return false, err
		}
		if child.kind != xmlText {
			continue
		}
		if err := d.budget.spend(1+(len(child.value)+len(value))/64, 0); err != nil {
			return false, err
		}
		if strings.EqualFold(child.value, value) {
			return true, nil
		}
	}
	return false, nil
}
func (d *xmlFragment) predicateMatches(id int, p xmlPredicate) (bool, error) {
	for c := d.nodes[id].first; c >= 0; c = d.nodes[c].next {
		n := d.nodes[c]
		if err := d.budget.spend(1+len(n.name)/64, 0); err != nil {
			return false, err
		}
		if n.name != p.name || (p.attribute && n.kind != xmlAttribute) || (!p.attribute && n.kind != xmlElement) {
			continue
		}
		if p.exists {
			return true, nil
		}
		ok, err := d.predicateLiteralMatches(c, p.value)
		if err != nil {
			return false, err
		}
		if ok {
			return true, nil
		}
	}
	return false, nil
}
func xmlCompare(left, right float64, op string) bool {
	switch op {
	case "=":
		return left == right
	case "!=":
		return left != right
	case "<":
		return left < right
	case "<=":
		return left <= right
	case ">":
		return left > right
	case ">=":
		return left >= right
	}
	return false
}
func (o xmlPositionOperand) at(position, size int) float64 {
	switch o.kind {
	case 'p':
		return float64(position)
	case 'l':
		return float64(size)
	default:
		return o.value
	}
}
func (d *xmlFragment) candidates(id int, s xmlStep) ([]int, error) {
	var out []int
	var err error
	if s.text {
		for c := d.nodes[id].first; c >= 0; c = d.nodes[c].next {
			n := d.nodes[c]
			if err = d.budget.spend(1, 0); err != nil {
				return nil, err
			}
			if n.kind == xmlText {
				out, err = d.appendCandidate(out, c)
				if err != nil {
					return nil, err
				}
			}
		}
	} else if s.axis == 's' {
		out, err = d.appendCandidate(out, id)
	} else if s.axis == 'p' {
		if d.nodes[id].parent >= 0 {
			out, err = d.appendCandidate(out, d.nodes[id].parent)
		}
	} else {
		for c := d.nodes[id].first; c >= 0; c = d.nodes[c].next {
			n := d.nodes[c]
			if err = d.budget.spend(1+len(n.name)/64, 0); err != nil {
				return nil, err
			}
			kind := xmlElement
			if s.axis == 'a' {
				kind = xmlAttribute
			}
			if n.kind == kind && (s.name == "*" || s.name == n.name) {
				out, err = d.appendCandidate(out, c)
				if err != nil {
					return nil, err
				}
			}
		}
	}
	if err != nil {
		return nil, err
	}
	return out, nil
}

type xmlCandidate struct {
	id, position int
}

func (d *xmlFragment) candidateMatches(id, position, size int, p xmlPredicate) (bool, error) {
	switch {
	case p.position == -1:
		return position == size, nil
	case p.position != 0:
		return position == p.position, nil
	case p.posCompare != nil:
		return xmlCompare(p.posCompare.left.at(position, size), p.posCompare.right.at(position, size), p.posCompare.op), nil
	default:
		return d.predicateMatches(id, p)
	}
}

// With one axis candidate group, its positions are already the slice indices.
func (d *xmlFragment) filterSingleCandidates(candidates []int, predicates []xmlPredicate) ([]int, error) {
	for _, p := range predicates {
		if len(candidates) == 0 {
			break
		}
		size := len(candidates)
		filtered := candidates[:0]
		for i, id := range candidates {
			if err := d.budget.spend(1, 0); err != nil {
				return nil, err
			}
			match, err := d.candidateMatches(id, i+1, size, p)
			if err != nil {
				return nil, err
			}
			if match {
				filtered = append(filtered, id)
			}
		}
		candidates = filtered
	}
	return candidates, nil
}

func (d *xmlFragment) filterCandidates(candidates []xmlCandidate, predicates []xmlPredicate) ([]xmlCandidate, error) {
	for _, p := range predicates {
		if len(candidates) == 0 {
			break
		}
		size := len(candidates)
		filtered := candidates[:0]
		for _, candidate := range candidates {
			if err := d.budget.spend(1, 0); err != nil {
				return nil, err
			}
			match, err := d.candidateMatches(candidate.id, candidate.position, size, p)
			if err != nil {
				return nil, err
			}
			if match {
				candidate.position = len(filtered) + 1
				filtered = append(filtered, candidate)
			}
		}
		candidates = filtered
	}
	return candidates, nil
}

func (d *xmlFragment) evaluate(p *xmlXPath) ([]int, error) {
	return d.evaluatePaths(p.paths)
}

func (d *xmlFragment) evaluatePaths(paths []xmlPath) ([]int, error) {
	if err := d.budget.spend(1, 2*len(d.nodes)); err != nil {
		return nil, err
	}
	selected := make([]bool, len(d.nodes))
	seen := make([]bool, len(d.nodes))
	for _, path := range paths {
		current := []int{0}
		for _, step := range path.steps {
			clear(seen)
			var records []xmlCandidate
			singleGroup := len(current) == 1 && !step.descendant
			for _, id := range current {
				apply := func(parent int) error {
					candidates, err := d.candidates(parent, step)
					if err != nil {
						return err
					}
					if singleGroup && len(step.predicates) > 0 {
						candidates, err = d.filterSingleCandidates(candidates, step.predicates)
						if err != nil {
							return err
						}
					}
					for i, c := range candidates {
						if len(step.predicates) == 0 || singleGroup || step.axis == 'p' {
							seen[c] = true
							continue
						}
						// The child and attribute axes number matching siblings per
						// parent. Predicates then see the whole step's candidate set.
						if err := d.budget.spend(1, 32); err != nil {
							return err
						}
						records = append(records, xmlCandidate{c, i + 1})
					}
					return nil
				}
				// The abbreviated // form expands the current context through
				// descendant-or-self. Evaluate self first for every node kind;
				// this is required for paths such as /a/@k//. where the
				// attribute is a leaf. Only elements can provide strict XML
				// descendants, so attribute and text records are not traversed.
				if err := d.budget.spend(1, 0); err != nil {
					return nil, err
				}
				if err := apply(id); err != nil {
					return nil, err
				}
				if !step.descendant || (d.nodes[id].kind != xmlElement && d.nodes[id].kind != xmlDocument) {
					continue
				}
				for parent := id + 1; parent < d.nodes[id].subtreeEnd; parent++ {
					if err := d.budget.spend(1, 0); err != nil {
						return nil, err
					}
					if d.nodes[parent].kind != xmlElement {
						continue
					}
					if err := apply(parent); err != nil {
						return nil, err
					}
				}
			}
			if len(step.predicates) > 0 && !singleGroup {
				if step.axis == 'p' {
					// The parent axis has a unique, document-ordered input set.
					for id, ok := range seen {
						if !ok {
							continue
						}
						if err := d.budget.spend(1, 32); err != nil {
							return nil, err
						}
						records = append(records, xmlCandidate{id, len(records) + 1})
					}
				}
				clear(seen)
				filtered, err := d.filterCandidates(records, step.predicates)
				if err != nil {
					return nil, err
				}
				for _, candidate := range filtered {
					seen[candidate.id] = true
				}
			}
			current = nil
			for id, ok := range seen {
				if err := d.budget.spend(1, 0); err != nil {
					return nil, err
				}
				if ok {
					var err error
					current, err = d.appendCandidate(current, id)
					if err != nil {
						return nil, err
					}
				}
			}
		}
		for _, id := range current {
			selected[id] = true
		}
	}
	var out []int
	for id, ok := range selected {
		if ok {
			var err error
			out, err = d.appendCandidate(out, id)
			if err != nil {
				return nil, err
			}
		}
	}
	return out, nil
}

func (d *xmlFragment) numericText(value string) (float64, error) {
	if err := d.budget.spend(1+len(value)/64, 0); err != nil {
		return 0, err
	}
	p := xmlPathParser{s: value}
	n, ok := p.decimal()
	if !ok {
		return 0, nil
	}
	return n, nil
}

func (d *xmlFragment) numericValue(operand *xmlNumericOperand, ids []int) (float64, error) {
	if operand.kind == 'n' {
		return operand.value, nil
	}
	if operand.kind == 'c' {
		return float64(len(ids)), nil
	}
	var total float64
	for _, id := range ids {
		n := d.nodes[id]
		if n.kind == xmlAttribute {
			v, err := d.numericText(n.value)
			if err != nil {
				return 0, err
			}
			total += v
		} else {
			for c := n.first; c >= 0; c = d.nodes[c].next {
				if err := d.budget.spend(1, 0); err != nil {
					return 0, err
				}
				if d.nodes[c].kind != xmlText {
					continue
				}
				v, err := d.numericText(d.nodes[c].value)
				if err != nil {
					return 0, err
				}
				total += v
			}
		}
		if math.IsInf(total, 0) || math.IsNaN(total) {
			return 0, moerr.NewInvalidInput(d.budget.ctx, "XPath numeric result out of range")
		}
	}
	return total, nil
}

func xmlExactInteger(operand *xmlNumericOperand, count int) (int64, bool) {
	if operand.kind == 'c' {
		return int64(count), true
	}
	if operand.kind != 'n' {
		return 0, false
	}
	return operand.integer, true
}

func xmlNumericString(value float64) string {
	if value == 0 {
		return "0"
	}
	var buf [mysqlFloat64MaxStringLength]byte
	return string(appendMySQLNumericFloat(buf[:0], value, 64))
}

func (d *xmlFragment) extract(p *xmlXPath, ids []int) (string, error) {
	if p.left != nil {
		left, err := d.numericValue(p.left, ids)
		if err != nil {
			return "", err
		}
		if p.op != "" {
			rightIDs := []int(nil)
			if p.right.kind != 'n' {
				rightIDs, err = d.evaluatePaths(p.right.paths)
				if err != nil {
					return "", err
				}
			}
			right, err := d.numericValue(p.right, rightIDs)
			if err != nil {
				return "", err
			}
			if leftInt, ok := xmlExactInteger(p.left, len(ids)); ok {
				if rightInt, ok := xmlExactInteger(p.right, len(rightIDs)); ok {
					comparison := 0
					if leftInt < rightInt {
						comparison = -1
					} else if leftInt > rightInt {
						comparison = 1
					}
					if xmlCompare(float64(comparison), 0, p.op) {
						return "1", nil
					}
					return "0", nil
				}
			}
			if xmlCompare(left, right, p.op) {
				return "1", nil
			}
			return "0", nil
		}
		if p.left.kind == 'n' {
			return strconv.FormatInt(p.left.integer, 10), nil
		}
		if p.left.kind == 'c' {
			return strconv.Itoa(len(ids)), nil
		}
		if left == 0 {
			return "0", nil
		}
		return xmlNumericString(left), nil
	}
	var out strings.Builder
	if err := d.budget.spend(1, len(d.nodes)); err != nil {
		return "", err
	}
	selected := make([]bool, len(d.nodes))
	for _, id := range ids {
		selected[id] = true
	}
	// Walk text records, not selected elements: /a|/a/b must interleave a's
	// surrounding text with b's text in original document order.
	for id, n := range d.nodes {
		if err := d.budget.spend(1, 0); err != nil {
			return "", err
		}
		v := ""
		if n.kind == xmlAttribute && selected[id] {
			v = n.value
		}
		if n.kind == xmlText && (selected[id] || selected[n.parent]) {
			v = n.value
		}
		if v == "" {
			continue
		}
		if out.Len()+len(v)+1 > xmlOutputLimit {
			return "", moerr.NewInvalidInput(d.budget.ctx, "XML output limit exceeded")
		}
		if err := d.budget.spend(1+len(v)/64, 2*(len(v)+1)); err != nil {
			return "", err
		}
		if out.Len() > 0 {
			out.WriteByte(' ')
		}
		out.WriteString(v)
	}
	return out.String(), nil
}
