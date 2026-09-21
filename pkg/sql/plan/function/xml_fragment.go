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
	"errors"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

const (
	xmlInputLimit   = 8 << 20
	xmlOutputLimit  = 16 << 20
	xmlScratchLimit = 32 << 20
	xmlRecordLimit  = 65536
	xmlDepthLimit   = 256
	xmlWorkLimit    = 1000000
)

var errXMLMalformed = errors.New("malformed XML fragment")

// Scratch is conservatively charged for slice capacity growth, not merely
// logical length. It belongs to one row; input strings remain borrowed.
type xmlBudget struct {
	ctx           context.Context
	work, scratch int
}

func (b *xmlBudget) spend(work, memory int) error {
	if err := b.ctx.Err(); err != nil {
		return err
	}
	if work > xmlWorkLimit-b.work || memory > xmlScratchLimit-b.scratch {
		return moerr.NewInvalidInput(b.ctx, "XML function resource limit exceeded")
	}
	b.work += work
	b.scratch += memory
	return nil
}

const (
	xmlDocument byte = iota
	xmlElement
	xmlAttribute
	xmlText
)

type xmlNode struct {
	kind                      byte
	name, value               string
	parent, first, next, last int
	start, end, subtreeEnd    int
}

type xmlFragment struct {
	source string
	nodes  []xmlNode
	budget *xmlBudget
}

func (d *xmlFragment) add(n xmlNode) (int, error) {
	if len(d.nodes) >= xmlRecordLimit {
		return 0, moerr.NewInvalidInput(d.budget.ctx, "XML node limit exceeded")
	}
	if err := d.budget.spend(1, 256); err != nil {
		return 0, err
	}
	n.first, n.next, n.last = -1, -1, -1
	id := len(d.nodes)
	d.nodes = append(d.nodes, n)
	if id > 0 {
		p := &d.nodes[n.parent]
		if p.last >= 0 {
			d.nodes[p.last].next = id
		} else {
			p.first = id
		}
		p.last = id
	}
	return id, nil
}

func xmlSpace(c byte) bool { return c == ' ' || c == '\t' || c == '\r' || c == '\n' }

func xmlNameEnd(s string, pos int) int {
	start := pos
	for pos < len(s) {
		r, n := utf8.DecodeRuneInString(s[pos:])
		if !(unicode.IsLetter(r) || r == '_' || r == ':' ||
			(pos > start && (unicode.IsDigit(r) || r == '-' || r == '.' || unicode.IsMark(r)))) {
			break
		}
		pos += n
	}
	return pos
}

func parseXMLFragment(ctx context.Context, source string) (*xmlFragment, error) {
	if len(source) > xmlInputLimit {
		return nil, moerr.NewInvalidInput(ctx, "XML input exceeds 8 MiB")
	}
	b := &xmlBudget{ctx: ctx}
	if err := b.spend(0, len(source)+16*xmlXPathLimit); err != nil {
		return nil, err
	}
	// Validate XML characters without interpreting entity references. The MySQL
	// oracle preserves even unknown entities literally.
	for i, nextCheck := 0, 0; i < len(source); {
		if i >= nextCheck {
			if err := b.spend(1, 0); err != nil {
				return nil, err
			}
			nextCheck = i + 4096
		}
		r, n := utf8.DecodeRuneInString(source[i:])
		if (r == utf8.RuneError && n == 1) || (r < 32 && r != 9 && r != 10 && r != 13) || r == 0xfffe || r == 0xffff {
			return nil, errXMLMalformed
		}
		i += n
	}
	d := &xmlFragment{source: source, budget: b}
	if _, err := d.add(xmlNode{kind: xmlDocument, parent: -1, end: len(source)}); err != nil {
		return nil, err
	}
	stack := []int{0}
	for pos := 0; pos < len(source); {
		if err := b.spend(1, 0); err != nil {
			return nil, err
		}
		parent := stack[len(stack)-1]
		start := pos
		if source[pos] != '<' {
			for pos < len(source) && source[pos] != '<' {
				if pos%4096 == 0 {
					if err := b.spend(1, 0); err != nil {
						return nil, err
					}
				}
				pos++
			}
			if strings.Contains(source[start:pos], "]]>") {
				return nil, errXMLMalformed
			}
			if _, err := d.add(xmlNode{kind: xmlText, parent: parent, start: start, end: pos, value: source[start:pos]}); err != nil {
				return nil, err
			}
			continue
		}
		if strings.HasPrefix(source[pos:], "<!--") || strings.HasPrefix(source[pos:], "<![CDATA[") || strings.HasPrefix(source[pos:], "<?") {
			prefix, suffix := 4, "-->"
			text := false
			if strings.HasPrefix(source[pos:], "<![CDATA[") {
				prefix, suffix, text = 9, "]]>", true
			} else if strings.HasPrefix(source[pos:], "<?") {
				prefix, suffix = 2, "?>"
			}
			end := strings.Index(source[pos+prefix:], suffix)
			if end < 0 {
				return nil, errXMLMalformed
			}
			value := source[pos+prefix : pos+prefix+end]
			if suffix == "-->" && strings.Contains(value, "--") {
				return nil, errXMLMalformed
			}
			pos += prefix + end + len(suffix)
			if err := b.spend(1+len(value)/4096, 0); err != nil {
				return nil, err
			}
			if text && len(value) > 0 {
				if _, err := d.add(xmlNode{kind: xmlText, parent: parent, start: start, end: pos, value: value}); err != nil {
					return nil, err
				}
			}
			continue
		}
		pos++
		closing := pos < len(source) && source[pos] == '/'
		if closing {
			pos++
		}
		nameStart := pos
		pos = xmlNameEnd(source, pos)
		if pos == nameStart {
			return nil, errXMLMalformed
		} // Includes DTD: no expansion.
		name := source[nameStart:pos]
		if closing {
			for pos < len(source) && xmlSpace(source[pos]) {
				pos++
			}
			if pos >= len(source) || source[pos] != '>' || len(stack) == 1 || d.nodes[parent].name != name {
				return nil, errXMLMalformed
			}
			pos++
			d.nodes[parent].end, d.nodes[parent].subtreeEnd = pos, len(d.nodes)
			stack = stack[:len(stack)-1]
			continue
		}
		if len(stack) > xmlDepthLimit {
			return nil, moerr.NewInvalidInput(ctx, "XML nesting limit exceeded")
		}
		id, err := d.add(xmlNode{kind: xmlElement, name: name, parent: parent, start: start})
		if err != nil {
			return nil, err
		}
		selfClosing := false
		for {
			beforeSpace := pos
			for pos < len(source) && xmlSpace(source[pos]) {
				pos++
			}
			if pos >= len(source) {
				return nil, errXMLMalformed
			}
			if source[pos] == '>' {
				pos++
				break
			}
			if source[pos] == '/' && pos+1 < len(source) && source[pos+1] == '>' {
				pos += 2
				selfClosing = true
				break
			}
			if pos == beforeSpace {
				return nil, errXMLMalformed
			}
			attrStart := pos
			pos = xmlNameEnd(source, pos)
			if pos == attrStart {
				return nil, errXMLMalformed
			}
			attr := source[attrStart:pos]
			for a := d.nodes[id].first; a >= 0; a = d.nodes[a].next {
				if err := b.spend(1+len(attr)/64, 0); err != nil {
					return nil, err
				}
				if d.nodes[a].name == attr {
					return nil, errXMLMalformed
				}
			}
			for pos < len(source) && xmlSpace(source[pos]) {
				pos++
			}
			if pos >= len(source) || source[pos] != '=' {
				return nil, errXMLMalformed
			}
			pos++
			for pos < len(source) && xmlSpace(source[pos]) {
				pos++
			}
			if pos >= len(source) || (source[pos] != '\'' && source[pos] != '"') {
				return nil, errXMLMalformed
			}
			quote := source[pos]
			pos++
			valueStart := pos
			for pos < len(source) && source[pos] != quote {
				if source[pos] == '<' {
					return nil, errXMLMalformed
				}
				if pos%4096 == 0 {
					if err := b.spend(1, 0); err != nil {
						return nil, err
					}
				}
				pos++
			}
			if pos >= len(source) {
				return nil, errXMLMalformed
			}
			value := source[valueStart:pos]
			pos++
			if _, err = d.add(xmlNode{kind: xmlAttribute, name: attr, value: value, parent: id, start: attrStart, end: pos}); err != nil {
				return nil, err
			}
		}
		if selfClosing {
			d.nodes[id].end, d.nodes[id].subtreeEnd = pos, len(d.nodes)
		} else {
			stack = append(stack, id)
		}
	}
	if len(stack) != 1 {
		return nil, errXMLMalformed
	}
	d.nodes[0].subtreeEnd = len(d.nodes)
	return d, nil
}

func (d *xmlFragment) text(id int) (string, error) {
	n := d.nodes[id]
	if n.kind == xmlAttribute {
		return n.value, nil
	}
	var out strings.Builder
	for child := n.first; child >= 0; child = d.nodes[child].next {
		v := d.nodes[child]
		if err := d.budget.spend(1, 0); err != nil {
			return "", err
		}
		if v.kind != xmlText || v.value == "" {
			continue
		}
		if err := d.budget.spend(1+len(v.value)/64, 2*(len(v.value)+1)); err != nil {
			return "", err
		}
		if out.Len() > 0 {
			out.WriteByte(' ')
		}
		out.WriteString(v.value)
	}
	return out.String(), nil
}
