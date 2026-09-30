// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package collation

import (
	"bytes"
	"encoding/binary"
	"unicode/utf8"

	"vitess.io/vitess/go/mysql/collations/colldata"
)

// MySQL's partial UCA 4.0.0 semantics, NOT modern Unicode normalization.
// Supplementary characters use the MySQL 0xfffd weight. The dependency is
// pinned in go.mod; changing it must preserve the independent oracle fixtures.
var uca400 = colldata.Lookup(224)

const uca400Space uint16 = 0x0209

func init() {
	if uca400 == nil || !bytes.Equal(uca400.WeightString(nil, []byte(" "), 0), []byte{2, 9}) {
		panic("pinned UCA400 backend or SPACE weight changed")
	}
}

// The pinned Vitess table treats U+FFFF as ignorable, unlike MySQL 8.4.11's
// implicit UCA4 weight FBC1 FFFF. ID224 has no contractions, so splitting at
// this scalar preserves surrounding context. Inputs have been UTF-8 checked.
func uca400Weights(dst, value []byte) []byte {
	for {
		i := bytes.Index(value, []byte("\uffff"))
		if i < 0 {
			return uca400.WeightString(dst, value, 0)
		}
		dst = uca400.WeightString(dst, value[:i], 0)
		dst = append(dst, 0xfb, 0xc1, 0xff, 0xff)
		value = value[i+3:]
	}
}

func (d Domain) uca400Key(scratch, value []byte) ([]byte, error) {
	if _, err := d.KeySizeUpperBound(len(value)); err != nil {
		return nil, err
	}
	if !utf8.Valid(value) {
		return nil, ErrUTF8
	}
	if d == UTF8MB3UnicodeCI {
		for _, r := range string(value) {
			if r > 0xffff {
				return nil, ErrRepertoire
			}
		}
	}
	// Keep weights separate from scratch so expansion never overwrites unread
	// weights. Both allocations are bounded by the checked scalar-size bound.
	weights := uca400Weights(nil, value)
	if len(weights)%2 != 0 {
		return nil, ErrKey
	}
	out := scratch[:0]
	spaces := 0
	for i := 0; i < len(weights); i += 2 {
		w := binary.BigEndian.Uint16(weights[i:])
		if w == uca400Space {
			spaces++
			continue
		}
		marker, tag := byte(0x29), byte(0x30)
		if w < uca400Space {
			marker, tag = 0x19, 0x10
		}
		for ; spaces > 0; spaces-- {
			out = append(out, marker)
		}
		out = append(out, tag, byte(w>>8), byte(w))
	}
	// Implicit infinite SPACE padding: omit trailing space weights and place
	// the terminator between lower and higher weights (not at byte zero).
	return append(out, 0x20), nil
}

// Check canonical framing, not whether the payload has a Unicode preimage.
// As with the 0900 backend, this is not a charset/repertoire validator.
func validateUCA400Key(key []byte) error {
	var pending byte
	for i := 0; i < len(key); {
		tag := key[i]
		i++
		switch tag {
		case 0x20:
			if i == len(key) && pending == 0 {
				return nil
			}
			return ErrKey
		case 0x19, 0x29:
			if pending != 0 && pending != tag {
				return ErrKey
			}
			pending = tag
		case 0x10, 0x30:
			if len(key)-i < 2 {
				return ErrKey
			}
			w := binary.BigEndian.Uint16(key[i:])
			i += 2
			if w == 0 || w == uca400Space ||
				(tag == 0x10 && (w > uca400Space || pending == 0x29)) ||
				(tag == 0x30 && (w < uca400Space || pending == 0x19)) {
				return ErrKey
			}
			pending = 0
		default:
			return ErrKey
		}
	}
	return ErrKey
}
