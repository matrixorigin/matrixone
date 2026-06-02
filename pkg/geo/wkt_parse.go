// Copyright 2021 - 2024 Matrix Origin
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

package geo

import (
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// ParseWKT parses a Well-Known Text representation into a Geometry. It accepts
// all seven geometry kinds, the "<TYPE> EMPTY" forms, and nested
// GEOMETRYCOLLECTION. Parsing is case-insensitive and whitespace-tolerant.
//
// Only 2D geometries are supported; a Z (elevation) or M (measure) ordinate is
// rejected (per gis.md, POINTZ/POINTM and friends are out of scope).
func ParseWKT(s string) (Geometry, error) {
	p := &wktParser{s: s}
	g, err := p.parseGeometry()
	if err != nil {
		return nil, err
	}
	if !p.eof() {
		return nil, p.errf("unexpected trailing characters")
	}
	return g, nil
}

type wktParser struct {
	s   string
	pos int
}

func (p *wktParser) parseGeometry() (Geometry, error) {
	kw, err := p.parseKeyword()
	if err != nil {
		return nil, err
	}
	switch kw {
	case "POINT":
		return p.parsePoint()
	case "LINESTRING":
		return p.parseLineString()
	case "POLYGON":
		return p.parsePolygon()
	case "MULTIPOINT":
		return p.parseMultiPoint()
	case "MULTILINESTRING":
		return p.parseMultiLineString()
	case "MULTIPOLYGON":
		return p.parseMultiPolygon()
	case "GEOMETRYCOLLECTION":
		return p.parseGeometryCollection()
	default:
		return nil, p.errf("unknown geometry type %q", kw)
	}
}

func (p *wktParser) parsePoint() (Geometry, error) {
	if p.tryKeyword("EMPTY") {
		return Point{IsEmpty: true}, nil
	}
	if err := p.expectByte('('); err != nil {
		return nil, err
	}
	c, err := p.parseCoord()
	if err != nil {
		return nil, err
	}
	if err := p.expectByte(')'); err != nil {
		return nil, err
	}
	return Point{X: c.X, Y: c.Y}, nil
}

func (p *wktParser) parseLineString() (Geometry, error) {
	if p.tryKeyword("EMPTY") {
		return LineString{}, nil
	}
	pts, err := p.parseCoordSeq()
	if err != nil {
		return nil, err
	}
	return LineString{Points: pts}, nil
}

func (p *wktParser) parsePolygon() (Geometry, error) {
	if p.tryKeyword("EMPTY") {
		return Polygon{}, nil
	}
	rings, err := p.parseRingSeq()
	if err != nil {
		return nil, err
	}
	return Polygon{Rings: rings}, nil
}

func (p *wktParser) parseMultiPoint() (Geometry, error) {
	if p.tryKeyword("EMPTY") {
		return MultiPoint{}, nil
	}
	if err := p.expectByte('('); err != nil {
		return nil, err
	}
	var pts []Point
	for {
		switch {
		case p.tryKeyword("EMPTY"):
			pts = append(pts, Point{IsEmpty: true})
		case p.peekByte() == '(':
			// Parenthesized form: MULTIPOINT((1 2), (3 4))
			_ = p.expectByte('(')
			c, err := p.parseCoord()
			if err != nil {
				return nil, err
			}
			if err := p.expectByte(')'); err != nil {
				return nil, err
			}
			pts = append(pts, Point{X: c.X, Y: c.Y})
		default:
			// Bare form: MULTIPOINT(1 2, 3 4)
			c, err := p.parseCoord()
			if err != nil {
				return nil, err
			}
			pts = append(pts, Point{X: c.X, Y: c.Y})
		}
		if p.consumeByte(',') {
			continue
		}
		break
	}
	if err := p.expectByte(')'); err != nil {
		return nil, err
	}
	return MultiPoint{Points: pts}, nil
}

func (p *wktParser) parseMultiLineString() (Geometry, error) {
	if p.tryKeyword("EMPTY") {
		return MultiLineString{}, nil
	}
	if err := p.expectByte('('); err != nil {
		return nil, err
	}
	var lines []LineString
	for {
		if p.tryKeyword("EMPTY") {
			lines = append(lines, LineString{})
		} else {
			pts, err := p.parseCoordSeq()
			if err != nil {
				return nil, err
			}
			lines = append(lines, LineString{Points: pts})
		}
		if p.consumeByte(',') {
			continue
		}
		break
	}
	if err := p.expectByte(')'); err != nil {
		return nil, err
	}
	return MultiLineString{Lines: lines}, nil
}

func (p *wktParser) parseMultiPolygon() (Geometry, error) {
	if p.tryKeyword("EMPTY") {
		return MultiPolygon{}, nil
	}
	if err := p.expectByte('('); err != nil {
		return nil, err
	}
	var polys []Polygon
	for {
		if p.tryKeyword("EMPTY") {
			polys = append(polys, Polygon{})
		} else {
			rings, err := p.parseRingSeq()
			if err != nil {
				return nil, err
			}
			polys = append(polys, Polygon{Rings: rings})
		}
		if p.consumeByte(',') {
			continue
		}
		break
	}
	if err := p.expectByte(')'); err != nil {
		return nil, err
	}
	return MultiPolygon{Polygons: polys}, nil
}

func (p *wktParser) parseGeometryCollection() (Geometry, error) {
	if p.tryKeyword("EMPTY") {
		return GeometryCollection{}, nil
	}
	if err := p.expectByte('('); err != nil {
		return nil, err
	}
	var geoms []Geometry
	for {
		g, err := p.parseGeometry()
		if err != nil {
			return nil, err
		}
		geoms = append(geoms, g)
		if p.consumeByte(',') {
			continue
		}
		break
	}
	if err := p.expectByte(')'); err != nil {
		return nil, err
	}
	return GeometryCollection{Geometries: geoms}, nil
}

// parseCoordSeq parses "( coord {, coord} )".
func (p *wktParser) parseCoordSeq() ([]Coord, error) {
	if err := p.expectByte('('); err != nil {
		return nil, err
	}
	var coords []Coord
	for {
		c, err := p.parseCoord()
		if err != nil {
			return nil, err
		}
		coords = append(coords, c)
		if p.consumeByte(',') {
			continue
		}
		break
	}
	if err := p.expectByte(')'); err != nil {
		return nil, err
	}
	return coords, nil
}

// parseRingSeq parses "( ring {, ring} )" where each ring is a coord sequence.
func (p *wktParser) parseRingSeq() ([][]Coord, error) {
	if err := p.expectByte('('); err != nil {
		return nil, err
	}
	var rings [][]Coord
	for {
		ring, err := p.parseCoordSeq()
		if err != nil {
			return nil, err
		}
		rings = append(rings, ring)
		if p.consumeByte(',') {
			continue
		}
		break
	}
	if err := p.expectByte(')'); err != nil {
		return nil, err
	}
	return rings, nil
}

// parseCoord parses two whitespace-separated ordinates "x y". A third ordinate
// (Z/M) is not consumed here, so it surfaces as a trailing-character error.
func (p *wktParser) parseCoord() (Coord, error) {
	x, err := p.parseNumber()
	if err != nil {
		return Coord{}, err
	}
	y, err := p.parseNumber()
	if err != nil {
		return Coord{}, err
	}
	return Coord{X: x, Y: y}, nil
}

func (p *wktParser) parseNumber() (float64, error) {
	p.skipWS()
	start := p.pos
	if p.pos < len(p.s) && (p.s[p.pos] == '+' || p.s[p.pos] == '-') {
		p.pos++
	}
	digits := false
	for p.pos < len(p.s) && isDigit(p.s[p.pos]) {
		p.pos++
		digits = true
	}
	if p.pos < len(p.s) && p.s[p.pos] == '.' {
		p.pos++
		for p.pos < len(p.s) && isDigit(p.s[p.pos]) {
			p.pos++
			digits = true
		}
	}
	if !digits {
		return 0, p.errf("expected number")
	}
	if p.pos < len(p.s) && (p.s[p.pos] == 'e' || p.s[p.pos] == 'E') {
		p.pos++
		if p.pos < len(p.s) && (p.s[p.pos] == '+' || p.s[p.pos] == '-') {
			p.pos++
		}
		expDigits := false
		for p.pos < len(p.s) && isDigit(p.s[p.pos]) {
			p.pos++
			expDigits = true
		}
		if !expDigits {
			return 0, p.errf("malformed number exponent")
		}
	}
	f, err := strconv.ParseFloat(p.s[start:p.pos], 64)
	if err != nil {
		return 0, p.errf("invalid number %q", p.s[start:p.pos])
	}
	return f, nil
}

func (p *wktParser) parseKeyword() (string, error) {
	p.skipWS()
	start := p.pos
	for p.pos < len(p.s) && isLetter(p.s[p.pos]) {
		p.pos++
	}
	if p.pos == start {
		return "", p.errf("expected geometry keyword")
	}
	return strings.ToUpper(p.s[start:p.pos]), nil
}

// tryKeyword consumes the next keyword token only if it matches kw
// (case-insensitively); otherwise the cursor is left untouched.
func (p *wktParser) tryKeyword(kw string) bool {
	save := p.pos
	p.skipWS()
	start := p.pos
	for p.pos < len(p.s) && isLetter(p.s[p.pos]) {
		p.pos++
	}
	if p.pos > start && strings.EqualFold(p.s[start:p.pos], kw) {
		return true
	}
	p.pos = save
	return false
}

func (p *wktParser) skipWS() {
	for p.pos < len(p.s) {
		switch p.s[p.pos] {
		case ' ', '\t', '\n', '\r', '\f', '\v':
			p.pos++
		default:
			return
		}
	}
}

func (p *wktParser) peekByte() byte {
	p.skipWS()
	if p.pos < len(p.s) {
		return p.s[p.pos]
	}
	return 0
}

func (p *wktParser) consumeByte(b byte) bool {
	p.skipWS()
	if p.pos < len(p.s) && p.s[p.pos] == b {
		p.pos++
		return true
	}
	return false
}

func (p *wktParser) expectByte(b byte) error {
	if p.consumeByte(b) {
		return nil
	}
	return p.errf("expected %q", string(b))
}

func (p *wktParser) eof() bool {
	p.skipWS()
	return p.pos >= len(p.s)
}

func (p *wktParser) errf(format string, args ...any) error {
	return moerr.NewInvalidInputNoCtxf("invalid WKT: "+format+" (at offset %d)", append(args, p.pos)...)
}

func isDigit(c byte) bool  { return c >= '0' && c <= '9' }
func isLetter(c byte) bool { return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') }
