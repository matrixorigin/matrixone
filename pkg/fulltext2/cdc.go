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

package fulltext2

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// The CDC-event codec: the ISCP consumer's Fulltext2SqlWriter accumulates a
// flush's source-row mutations into a Cdc, ships it as one channel blob (Encode),
// and RunFulltext2 decodes it (DecodeCdc) and STREAMS it through a TailBuilder
// into capacity-capped tag=1 CdcTail segments. The blob is BINARY (typed pk) —
// like bm25's WandCdc — because a fulltext2 pk is `any` (int64 OR varchar) and a
// JSON round-trip would corrupt a non-integer pk.

type cdcOp byte

const (
	cdcInsert cdcOp = 'I'
	cdcUpsert cdcOp = 'U'
	cdcDelete cdcOp = 'D'
)

// CdcEvent is one source-row mutation: an INSERT/UPSERT carries the row's text
// (tokenized at build) and its INCLUDE column values; a DELETE carries only the pk.
type CdcEvent struct {
	Op      cdcOp
	Pk      any
	Text    string
	Include []any // INCLUDE column values in column order (nil element = NULL); nil for DELETE
}

// Cdc is the per-flush CDC batch the sinker accumulates and ships as one blob.
// IncludeTypes is the index's INCLUDE column schema (types.T per column, column order),
// needed to typed-encode/decode each event's Include values; nil ⇒ no INCLUDE columns.
type Cdc struct {
	PkType       int32
	IncludeTypes []int32
	Events       []CdcEvent
}

func NewCdc(pkType int32) *Cdc { return &Cdc{PkType: pkType} }

func (c *Cdc) Insert(pk any, text string, include []any) {
	c.Events = append(c.Events, CdcEvent{cdcInsert, pk, text, include})
}
func (c *Cdc) Upsert(pk any, text string, include []any) {
	c.Events = append(c.Events, CdcEvent{cdcUpsert, pk, text, include})
}
func (c *Cdc) Delete(pk any) { c.Events = append(c.Events, CdcEvent{cdcDelete, pk, "", nil}) }
func (c *Cdc) Len() int      { return len(c.Events) }

const cdcMagic uint32 = 0x46540200 // 'F' 'T' 02 00 — fulltext2 cdc events

// Encode serializes the batch: magic | pkType | count |
// [op:1 pkLen:u32 pk textLen:u32 text]* | crc32. Self-describing + CRC-checked.
func (c *Cdc) Encode() ([]byte, error) {
	var b bytes.Buffer
	_ = binary.Write(&b, binary.LittleEndian, cdcMagic)
	_ = binary.Write(&b, binary.LittleEndian, c.PkType)
	// INCLUDE column schema: count + per-col type (0 ⇒ no INCLUDE columns). Lets the
	// decoder typed-decode each event's Include values without an external schema.
	_ = binary.Write(&b, binary.LittleEndian, uint32(len(c.IncludeTypes)))
	for _, t := range c.IncludeTypes {
		_ = binary.Write(&b, binary.LittleEndian, t)
	}
	_ = binary.Write(&b, binary.LittleEndian, int64(len(c.Events)))
	for _, e := range c.Events {
		pkb, err := encodePk(c.PkType, e.Pk)
		if err != nil {
			return nil, err
		}
		b.WriteByte(byte(e.Op))
		_ = binary.Write(&b, binary.LittleEndian, uint32(len(pkb)))
		b.Write(pkb)
		_ = binary.Write(&b, binary.LittleEndian, uint32(len(e.Text)))
		b.WriteString(e.Text)
		// INCLUDE values (column order); a missing/nil value is SQL NULL. Same
		// [nullFlag][u32 len][content] shape as the docmap include section.
		for ci, t := range c.IncludeTypes {
			var v any
			if ci < len(e.Include) {
				v = e.Include[ci]
			}
			if v == nil {
				b.WriteByte(includeNull)
				continue
			}
			vb, err := encodePk(t, v)
			if err != nil {
				return nil, err
			}
			b.WriteByte(includeNotNull)
			_ = binary.Write(&b, binary.LittleEndian, uint32(len(vb)))
			b.Write(vb)
		}
	}
	sum := crc32.ChecksumIEEE(b.Bytes())
	_ = binary.Write(&b, binary.LittleEndian, sum)
	return b.Bytes(), nil
}

// DecodeCdc reverses Encode, validating magic + CRC.
func DecodeCdc(buf []byte) (*Cdc, error) {
	if len(buf) < 4+4+8+4 {
		return nil, moerr.NewInternalErrorNoCtx("fulltext2 cdc: truncated")
	}
	body := buf[:len(buf)-4]
	if crc32.ChecksumIEEE(body) != binary.LittleEndian.Uint32(buf[len(buf)-4:]) {
		return nil, moerr.NewInternalErrorNoCtx("fulltext2 cdc: checksum mismatch")
	}
	r := bytes.NewReader(body)
	var magic uint32
	_ = binary.Read(r, binary.LittleEndian, &magic)
	if magic != cdcMagic {
		return nil, moerr.NewInternalErrorNoCtx("fulltext2 cdc: bad magic")
	}
	c := &Cdc{}
	if err := binary.Read(r, binary.LittleEndian, &c.PkType); err != nil {
		return nil, err
	}
	var nIncl uint32
	if err := binary.Read(r, binary.LittleEndian, &nIncl); err != nil {
		return nil, err
	}
	if nIncl > maxIncludeCols {
		return nil, moerr.NewInternalErrorNoCtx("fulltext2 cdc: bad include col count")
	}
	if nIncl > 0 {
		c.IncludeTypes = make([]int32, nIncl)
		for i := range c.IncludeTypes {
			if err := binary.Read(r, binary.LittleEndian, &c.IncludeTypes[i]); err != nil {
				return nil, err
			}
		}
	}
	var n int64
	if err := binary.Read(r, binary.LittleEndian, &n); err != nil {
		return nil, err
	}
	if n < 0 {
		return nil, moerr.NewInternalErrorNoCtx("fulltext2 cdc: bad count")
	}
	// n is read straight off a (possibly corrupt) blob; a bogus huge count would make()
	// gigabytes up front. Cap the pre-alloc hint — append grows it for a genuinely large
	// frame, and a truncated stream errors out per-event in the loop below.
	capHint := n
	if capHint > 4096 {
		capHint = 4096
	}
	c.Events = make([]CdcEvent, 0, capHint)
	for i := int64(0); i < n; i++ {
		op, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		pk, err := cdcReadLenBytes(r, c.PkType)
		if err != nil {
			return nil, err
		}
		text, err := cdcReadLenString(r)
		if err != nil {
			return nil, err
		}
		var include []any
		if nIncl > 0 {
			include = make([]any, nIncl)
			for ci := 0; ci < int(nIncl); ci++ {
				v, ierr := cdcReadInclude(r, c.IncludeTypes[ci])
				if ierr != nil {
					return nil, ierr
				}
				include[ci] = v
			}
		}
		c.Events = append(c.Events, CdcEvent{Op: cdcOp(op), Pk: pk, Text: text, Include: include})
	}
	return c, nil
}

// cdcReadInclude reads one INCLUDE value: a null flag, then (if not null) a u32 length +
// typed content decoded via decodePk. Returns nil for a NULL value.
func cdcReadInclude(r *bytes.Reader, t int32) (any, error) {
	flag, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	if flag == includeNull {
		return nil, nil
	}
	if flag != includeNotNull {
		return nil, moerr.NewInternalErrorNoCtx("fulltext2 cdc: bad include null-flag")
	}
	return cdcReadLenBytes(r, t)
}

func cdcReadLenBytes(r *bytes.Reader, pkType int32) (any, error) {
	var l uint32
	if err := binary.Read(r, binary.LittleEndian, &l); err != nil {
		return nil, err
	}
	if int64(l) > int64(r.Len()) {
		return nil, moerr.NewInternalErrorNoCtx("fulltext2 cdc: truncated pk")
	}
	pkb := make([]byte, l)
	if _, err := io.ReadFull(r, pkb); err != nil {
		return nil, err
	}
	return decodePk(pkType, pkb)
}

func cdcReadLenString(r *bytes.Reader) (string, error) {
	var l uint32
	if err := binary.Read(r, binary.LittleEndian, &l); err != nil {
		return "", err
	}
	if int64(l) > int64(r.Len()) {
		return "", moerr.NewInternalErrorNoCtx("fulltext2 cdc: truncated text")
	}
	sb := make([]byte, l)
	if _, err := io.ReadFull(r, sb); err != nil {
		return "", err
	}
	return string(sb), nil
}
