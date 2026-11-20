// Copyright 2022 Matrix Origin
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

package bytejson

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"strconv"
	"strings"
	"unicode/utf16"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/common/util"
)

const (
	jsonParseChunk = 64 << 10
	jsonParseItems = 256
	// The legacy parser searches preceding keys and sorts them on close.
	// Restrict its fast path to small inputs to bound that work.
	jsonParseFastLimit = 1024
	// Same limit enforced by json.Valid in ParseFromByteSlice.
	jsonParseDepthLimit = 10000
)

// ParseFromByteSliceWithCancellation parses a SQL expression's text document.
// The result owns its bytes, and cancellation is returned as ctx.Err().
func ParseFromByteSliceWithCancellation(ctx context.Context, data []byte) (ByteJson, error) {
	if ctx == nil || ctx.Done() == nil {
		return parseFromByteSliceWithContext(ctx, data)
	}
	if err := ctx.Err(); err != nil {
		return ByteJson{}, err
	}
	if len(data) <= jsonParseFastLimit {
		result, err := parseFromByteSliceWithContext(ctx, data)
		if err == nil {
			err = ctx.Err()
		}
		return result, err
	}

	p := cancelJSONParser{ctx: ctx, data: data, nextCheck: jsonParseChunk}
	n, err := p.value(0)
	if err != nil {
		return ByteJson{}, err
	}
	defer n.Free()
	if err := p.space(); err != nil {
		return ByteJson{}, err
	}
	if p.pos != len(data) {
		return ByteJson{}, p.syntaxError()
	}
	if err := ctx.Err(); err != nil {
		return ByteJson{}, err
	}

	w := byteJsonWriter{buf: make([]byte, 0, len(data)), cancel: &jsonWriteBudget{}}
	if _, _, err := w.writeNodeWithContext(ctx, true, n); err != nil {
		return ByteJson{}, err
	}
	if err := ctx.Err(); err != nil {
		return ByteJson{}, err
	}
	return ByteJson{Type: w.buf[0], Data: w.buf[1:]}, nil
}

type cancelJSONParser struct {
	ctx       context.Context
	data      []byte
	pos       int
	nextCheck int
	items     int
}

func (p *cancelJSONParser) syntaxError() error {
	return moerr.NewInvalidInputf(p.ctx, "invalid json text at byte %d", p.pos)
}

func (p *cancelJSONParser) check() error {
	if p.pos >= p.nextCheck || p.items >= jsonParseItems {
		p.nextCheck = p.pos + jsonParseChunk
		p.items = 0
		return p.ctx.Err()
	}
	return nil
}

func (p *cancelJSONParser) peek() byte {
	if p.pos == len(p.data) {
		return 0
	}
	return p.data[p.pos]
}

func (p *cancelJSONParser) space() error {
	for p.pos < len(p.data) {
		switch p.peek() {
		case ' ', '\t', '\r', '\n':
			p.pos++
			if err := p.check(); err != nil {
				return err
			}
		default:
			return nil
		}
	}
	return nil
}

func (p *cancelJSONParser) value(depth int) (Node, error) {
	p.items++
	if err := p.check(); err != nil {
		return Node{}, err
	}
	if err := p.space(); err != nil {
		return Node{}, err
	}
	switch p.peek() {
	case '{', '[':
		if depth == jsonParseDepthLimit {
			return Node{}, p.syntaxError()
		}
		return p.group(depth, p.peek() == '{')
	case '"':
		value, err := p.readString()
		return Node{value}, err
	case 'n':
		return p.literal("null", nil)
	case 'f':
		return p.literal("false", false)
	case 't':
		return p.literal("true", true)
	default:
		start := p.pos
		if err := p.number(); err != nil {
			return Node{}, err
		}
		return Node{json.Number(util.UnsafeBytesToString(p.data[start:p.pos]))}, nil
	}
}

func (p *cancelJSONParser) literal(word string, value any) (Node, error) {
	if !strings.HasPrefix(util.UnsafeBytesToString(p.data[p.pos:]), word) {
		return Node{}, p.syntaxError()
	}
	p.pos += len(word)
	return Node{value}, nil
}

func (p *cancelJSONParser) group(depth int, object bool) (result Node, err error) {
	p.pos++
	g := reuse.Alloc[Group](nil)
	g.Obj = object
	defer func() {
		if err != nil {
			g.free()
		}
	}()

	end := byte(']')
	if object {
		end = '}'
	}
	if err := p.space(); err != nil {
		return Node{}, err
	}
	if p.peek() == end {
		p.pos++
		return Node{g}, nil
	}

	// Binary JSON keys have a uint16 length. Oversized keys cannot produce
	// a value, so leave them for the writer to reject after syntax validation.
	index := make(map[string]int)
	for {
		var key string
		if object {
			if p.peek() != '"' {
				return Node{}, p.syntaxError()
			}
			key, err = p.readString()
			if err != nil {
				return Node{}, err
			}
			if err = p.space(); err != nil {
				return Node{}, err
			}
			if p.peek() != ':' {
				return Node{}, p.syntaxError()
			}
			p.pos++
		}

		value, err := p.value(depth + 1)
		if err != nil {
			return Node{}, err
		}
		if object && len(key) <= 65535 {
			if prior, ok := index[key]; ok {
				g.Values[prior].Free()
				g.Values[prior] = value
			} else {
				index[key] = len(g.Values)
				g.Keys = append(g.Keys, key)
				g.Values = append(g.Values, value)
			}
		} else {
			if object {
				g.Keys = append(g.Keys, key)
			}
			g.Values = append(g.Values, value)
		}

		if err := p.space(); err != nil {
			return Node{}, err
		}
		switch p.peek() {
		case end:
			p.pos++
			return Node{g}, nil
		case ',':
			p.pos++
			if err := p.space(); err != nil {
				return Node{}, err
			}
		default:
			return Node{}, p.syntaxError()
		}
	}
}

func (p *cancelJSONParser) digits() error {
	start := p.pos
	for b := p.peek(); b >= '0' && b <= '9'; b = p.peek() {
		p.pos++
		if err := p.check(); err != nil {
			return err
		}
	}
	if start == p.pos {
		return p.syntaxError()
	}
	return nil
}

func (p *cancelJSONParser) number() error {
	if p.peek() == '-' {
		p.pos++
	}
	if p.peek() == '0' {
		p.pos++
	} else if err := p.digits(); err != nil {
		return err
	}
	if p.peek() == '.' {
		p.pos++
		if err := p.digits(); err != nil {
			return err
		}
	}
	if b := p.peek(); b == 'e' || b == 'E' {
		p.pos++
		if b := p.peek(); b == '+' || b == '-' {
			p.pos++
		}
		return p.digits()
	}
	return nil
}

func (p *cancelJSONParser) readString() (string, error) {
	p.pos++
	start := p.pos
	for p.pos < len(p.data) {
		if err := p.check(); err != nil {
			return "", err
		}
		end := min(len(p.data), p.nextCheck)
		chunk := p.data[p.pos:end]
		quote := bytes.IndexByte(chunk, '"')
		if quote >= 0 {
			chunk = chunk[:quote]
		}
		if bytes.IndexByte(chunk, '\\') >= 0 || !printableJSONASCII(chunk) {
			// Count the decoded size before allocating so a long escaped
			// string can be written without growing and copying its prefix.
			p.pos = start
			p.nextCheck = start
			size, err := p.stringBody(nil)
			if err != nil {
				return "", err
			}
			decoded := make([]byte, size)
			p.pos = start
			p.nextCheck = start
			if _, err := p.stringBody(decoded); err != nil {
				return "", err
			}
			return util.UnsafeBytesToString(decoded), nil
		}
		p.pos += len(chunk)
		if quote >= 0 {
			value := util.UnsafeBytesToString(p.data[start:p.pos])
			p.pos++
			return value, nil
		}
	}
	return "", p.syntaxError()
}

// printableJSONASCII tests eight bytes at a time on the unescaped fast path.
func printableJSONASCII(data []byte) bool {
	const high = 0x8080808080808080
	const lowASCII = 0x2020202020202020
	const highASCII = 0x7e7e7e7e7e7e7e7e
	for len(data) >= 8 {
		v := binary.LittleEndian.Uint64(data)
		if (v|(v-lowASCII)|(highASCII-v))&high != 0 {
			return false
		}
		data = data[8:]
	}
	for _, b := range data {
		if b < ' ' || b > '~' {
			return false
		}
	}
	return true
}

func (p *cancelJSONParser) stringBody(dst []byte) (int, error) {
	size := 0
	for p.pos < len(p.data) {
		if err := p.check(); err != nil {
			return 0, err
		}
		var r rune
		switch b := p.peek(); {
		case b == '"':
			p.pos++
			return size, nil
		case b == '\\':
			p.pos++
			var ok bool
			r, ok = p.escape()
			if !ok {
				return 0, p.syntaxError()
			}
		case b < 0x20:
			return 0, p.syntaxError()
		default:
			var width int
			r, width = utf8.DecodeRune(p.data[p.pos:])
			p.pos += width
		}
		if dst != nil {
			size += utf8.EncodeRune(dst[size:], r)
		} else {
			size += utf8.RuneLen(r)
		}
	}
	return 0, p.syntaxError()
}

func (p *cancelJSONParser) escape() (rune, bool) {
	if p.pos == len(p.data) {
		return 0, false
	}
	b := p.peek()
	p.pos++
	switch b {
	case '"', '/', '\\':
		return rune(b), true
	case 'n':
		return '\n', true
	case 'r':
		return '\r', true
	case 't':
		return '\t', true
	case 'b':
		return '\b', true
	case 'f':
		return '\f', true
	case 'u':
		r, ok := hexJSONRune(p.data[p.pos:])
		if !ok {
			return 0, false
		}
		p.pos += 4
		if utf16.IsSurrogate(r) {
			if tail := p.data[p.pos:]; len(tail) >= 6 && tail[0] == '\\' && tail[1] == 'u' {
				if next, ok := hexJSONRune(tail[2:]); ok {
					if pair := utf16.DecodeRune(r, next); pair != utf8.RuneError {
						p.pos += 6
						return pair, true
					}
				}
			}
			r = utf8.RuneError
		}
		return r, true
	default:
		return 0, false
	}
}

func hexJSONRune(data []byte) (rune, bool) {
	if len(data) < 4 {
		return 0, false
	}
	var r rune
	for _, b := range data[:4] {
		r <<= 4
		switch {
		case '0' <= b && b <= '9':
			r += rune(b - '0')
		case 'a' <= b && b <= 'f':
			r += rune(b-'a') + 10
		case 'A' <= b && b <= 'F':
			r += rune(b-'A') + 10
		default:
			return 0, false
		}
	}
	return r, true
}

type jsonWriteBudget struct {
	bytes, items int
}

func (b *jsonWriteBudget) check(ctx context.Context, bytes int) error {
	b.bytes += bytes
	b.items++
	if b.bytes >= jsonParseChunk || b.items >= jsonParseItems {
		b.bytes, b.items = 0, 0
		return ctx.Err()
	}
	return nil
}

type jsonKeySort struct {
	*groupSortKeys
	ctx    context.Context
	budget *jsonWriteBudget
}

func (g jsonKeySort) less(i, j int) (bool, error) {
	a, b := g.Keys[i], g.Keys[j]
	n := min(len(a), len(b))
	for begin := 0; begin < n; begin += jsonParseChunk {
		end := min(n, begin+jsonParseChunk)
		if err := g.budget.check(g.ctx, end-begin); err != nil {
			return false, err
		}
		if cmp := strings.Compare(a[begin:end], b[begin:end]); cmp != 0 {
			return cmp < 0, nil
		}
	}
	if err := g.budget.check(g.ctx, 0); err != nil {
		return false, err
	}
	return len(a) < len(b), nil
}

// siftDown restores the max heap below root. Every swap follows a comparison,
// so the comparison budget also bounds work spent moving key/value pairs.
func (g jsonKeySort) siftDown(root, end int) error {
	for root < end/2 {
		child := 2*root + 1
		if child+1 < end {
			less, err := g.less(child, child+1)
			if err != nil {
				return err
			}
			if less {
				child++
			}
		}
		less, err := g.less(root, child)
		if err != nil || !less {
			return err
		}
		g.Swap(root, child)
		root = child
	}
	return nil
}

func sortJSONKeysCancellable(ctx context.Context, budget *jsonWriteBudget, group *Group) error {
	g := jsonKeySort{(*groupSortKeys)(group), ctx, budget}
	n := g.Len()
	i := 1
	for ; i < n; i++ {
		less, err := g.less(i, i-1)
		if err != nil {
			return err
		}
		if less {
			break
		}
	}
	if i == n {
		return nil
	}
	// Heap sort bounds worst-case work without allocating scratch space.
	for root := n/2 - 1; root >= 0; root-- {
		if err := g.siftDown(root, n); err != nil {
			return err
		}
	}
	for end := n - 1; end > 0; end-- {
		g.Swap(0, end)
		if err := g.siftDown(0, end); err != nil {
			return err
		}
	}
	return nil
}

// shortJSONFloat keeps enough significant digits to distinguish every binary64
// rounding midpoint. A nonzero discarded digit breaks an exact midpoint tie.
func shortJSONFloat(ctx context.Context, text string) (string, error) {
	const significant = 799
	var mantissa [significant + 2]byte
	n, decimal, first, digits := 0, 0, -1, 0
	hasPoint, sticky, truncated := false, false, false
	i := 0
	if text[0] == '-' {
		mantissa[0] = '-'
		n++
		i++
	}
	firstDigit := n
	for ; i < len(text) && text[i] != 'e' && text[i] != 'E'; i++ {
		if i%jsonParseChunk == 0 {
			if err := ctx.Err(); err != nil {
				return "", err
			}
		}
		digit := text[i]
		if digit == '.' {
			hasPoint = true
			continue
		}
		if !hasPoint {
			decimal++
		}
		if first < 0 && digit != '0' {
			first = digits
		}
		digits++
		if first < 0 {
			continue
		}
		if n-firstDigit >= 19 && digit != '0' {
			truncated = true
		}
		if n-firstDigit < significant {
			mantissa[n] = digit
			n++
		} else {
			sticky = sticky || digit != '0'
		}
	}
	exp := 0
	if i < len(text) {
		i++
		negative := text[i] == '-'
		if negative || text[i] == '+' {
			i++
		}
		for ; i < len(text); i++ {
			if i%jsonParseChunk == 0 {
				if err := ctx.Err(); err != nil {
					return "", err
				}
			}
			// Match strconv's bounded exponent accumulation.
			if exp < 10000 {
				exp = 10*exp + int(text[i]-'0')
			}
		}
		if negative {
			exp = -exp
		}
	}
	if first < 0 {
		return string(mantissa[:firstDigit]) + "0e0", nil
	}
	if sticky {
		mantissa[n] = '1'
		n++
	}
	before := decimal - first
	if before > significant+1 && truncated {
		// strconv's fast path uses the full decimal position but its slow
		// path counts at most 800 digits before the point. Reproduce the
		// fast-path decision by checking its two 19-digit endpoints.
		prefix := mantissa[firstDigit : firstDigit+19]
		d, _ := strconv.ParseUint(string(prefix), 10, 64)
		power := int64(max(-2000, min(2000, exp+before-19)))
		lower := string(strconv.AppendInt(append([]byte(string(prefix)), 'e'), power, 10))
		upper := string(strconv.AppendInt(append(strconv.AppendUint(nil, d+1, 10), 'e'), power, 10))
		lo, _ := strconv.ParseFloat(lower, 64)
		hi, _ := strconv.ParseFloat(upper, 64)
		if lo != hi {
			before = significant + 1
		}
	}
	exp += before - (n - firstDigit)
	// Extreme exponents can be represented by a short equivalent overflow
	// or underflow; the existing number writer chooses the error/value.
	exp = max(-2000, min(2000, exp))
	return string(strconv.AppendInt(append(mantissa[:n], 'e'), int64(exp), 10)), nil
}
