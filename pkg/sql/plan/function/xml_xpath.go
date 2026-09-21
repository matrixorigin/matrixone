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
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

const xmlXPathLimit = 16 << 10

type xmlPredicate struct {
	position          int // -1 is last(); zero is a name test.
	attribute, exists bool
	name, value       string
}
type xmlStep struct {
	name       string
	axis       byte // c child, a attribute, s self, p parent
	descendant bool
	predicates []xmlPredicate
}
type xmlPath struct {
	steps []xmlStep
	text  bool
}
type xmlXPath struct {
	paths []xmlPath
	count bool
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
	if strings.Contains(name, "::") {
		return ""
	}
	return name
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
		out.position = -1
	} else if p.take("position()") {
		if !p.take("=") {
			return out, p.failure()
		}
		n, ok := p.number()
		if !ok {
			return out, p.failure()
		}
		out.position = n
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
			// MySQL's ExtractValue text() is the direct-text extraction surface.
			// Do not pretend it is an element nodeset for count or replacement.
			if descendant {
				return out, p.failure()
			}
			out.text = true
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

func compileXMLXPath(ctx context.Context, s string) (*xmlXPath, error) {
	if len(s) > xmlXPathLimit {
		return nil, moerr.NewInvalidInput(ctx, "XPath exceeds 16 KiB")
	}
	p := xmlPathParser{ctx: ctx, s: s}
	out := &xmlXPath{}
	out.count = p.take("count(")
	for {
		path, err := p.path()
		if err != nil {
			return nil, err
		}
		if path.text && out.count {
			return nil, p.failure()
		}
		out.paths = append(out.paths, path)
		if !p.take("|") {
			break
		}
	}
	if out.count && !p.take(")") {
		return nil, p.failure()
	}
	p.space()
	if p.pos != len(s) {
		return nil, p.failure()
	}
	return out, nil
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
func (d *xmlFragment) candidates(id int, s xmlStep) ([]int, error) {
	var out []int
	var err error
	if s.axis == 's' {
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
	for _, p := range s.predicates {
		if len(out) == 0 {
			break
		}
		if err := d.budget.spend(1, 0); err != nil {
			return nil, err
		}
		if p.position != 0 {
			i := p.position - 1
			if p.position == -1 {
				i = len(out) - 1
			}
			if i < 0 || i >= len(out) {
				out = out[:0]
			} else {
				out = out[i : i+1]
			}
			continue
		}
		filtered := out[:0]
		for _, c := range out {
			ok, err := d.predicateMatches(c, p)
			if err != nil {
				return nil, err
			}
			if ok {
				filtered = append(filtered, c)
			}
		}
		out = filtered
	}
	return out, nil
}

func (d *xmlFragment) evaluate(p *xmlXPath) ([]int, error) {
	if err := d.budget.spend(1, 2*len(d.nodes)); err != nil {
		return nil, err
	}
	selected := make([]bool, len(d.nodes))
	seen := make([]bool, len(d.nodes))
	for _, path := range p.paths {
		current := []int{0}
		for _, step := range path.steps {
			clear(seen)
			for _, id := range current {
				apply := func(parent int) error {
					candidates, err := d.candidates(parent, step)
					if err != nil {
						return err
					}
					for _, c := range candidates {
						seen[c] = true
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

func (d *xmlFragment) extract(p *xmlXPath, ids []int) (string, error) {
	if p.count {
		return strconv.Itoa(len(ids)), nil
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
		if n.kind == xmlText && selected[n.parent] {
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
