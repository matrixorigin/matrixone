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
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
)

// WriteGeoJSON renders g as an RFC 7946 GeoJSON geometry object. When maxDec is
// >= 0 every coordinate is rounded to at most that many decimal places; a
// negative value keeps full round-trip precision.
func WriteGeoJSON(g Geometry, maxDec int) string {
	var b strings.Builder
	writeGeoJSON(&b, g, maxDec)
	return b.String()
}

func writeGeoJSON(b *strings.Builder, g Geometry, maxDec int) {
	switch v := g.(type) {
	case Point:
		b.WriteString(`{"type":"Point","coordinates":`)
		if v.IsEmpty {
			b.WriteString("[]")
		} else {
			gjWriteCoord(b, Coord{X: v.X, Y: v.Y}, maxDec)
		}
		b.WriteByte('}')
	case LineString:
		b.WriteString(`{"type":"LineString","coordinates":`)
		gjWriteCoordSeq(b, v.Points, maxDec)
		b.WriteByte('}')
	case Polygon:
		b.WriteString(`{"type":"Polygon","coordinates":`)
		gjWriteRings(b, v.Rings, maxDec)
		b.WriteByte('}')
	case MultiPoint:
		b.WriteString(`{"type":"MultiPoint","coordinates":`)
		coords := make([]Coord, 0, len(v.Points))
		for _, p := range v.Points {
			coords = append(coords, Coord{X: p.X, Y: p.Y})
		}
		gjWriteCoordSeq(b, coords, maxDec)
		b.WriteByte('}')
	case MultiLineString:
		b.WriteString(`{"type":"MultiLineString","coordinates":`)
		b.WriteByte('[')
		for i, l := range v.Lines {
			if i > 0 {
				b.WriteByte(',')
			}
			gjWriteCoordSeq(b, l.Points, maxDec)
		}
		b.WriteByte(']')
		b.WriteByte('}')
	case MultiPolygon:
		b.WriteString(`{"type":"MultiPolygon","coordinates":`)
		b.WriteByte('[')
		for i, p := range v.Polygons {
			if i > 0 {
				b.WriteByte(',')
			}
			gjWriteRings(b, p.Rings, maxDec)
		}
		b.WriteByte(']')
		b.WriteByte('}')
	case GeometryCollection:
		b.WriteString(`{"type":"GeometryCollection","geometries":[`)
		for i, sub := range v.Geometries {
			if i > 0 {
				b.WriteByte(',')
			}
			writeGeoJSON(b, sub, maxDec)
		}
		b.WriteString(`]}`)
	}
}

func gjWriteRings(b *strings.Builder, rings [][]Coord, maxDec int) {
	b.WriteByte('[')
	for i, r := range rings {
		if i > 0 {
			b.WriteByte(',')
		}
		gjWriteCoordSeq(b, r, maxDec)
	}
	b.WriteByte(']')
}

func gjWriteCoordSeq(b *strings.Builder, cs []Coord, maxDec int) {
	b.WriteByte('[')
	for i, c := range cs {
		if i > 0 {
			b.WriteByte(',')
		}
		gjWriteCoord(b, c, maxDec)
	}
	b.WriteByte(']')
}

func gjWriteCoord(b *strings.Builder, c Coord, maxDec int) {
	b.WriteByte('[')
	b.WriteString(fmtGeoJSONNum(c.X, maxDec))
	b.WriteByte(',')
	b.WriteString(fmtGeoJSONNum(c.Y, maxDec))
	b.WriteByte(']')
}

func fmtGeoJSONNum(v float64, maxDec int) string {
	if maxDec >= 0 {
		p := math.Pow(10, float64(maxDec))
		v = math.Round(v*p) / p
	}
	return strconv.FormatFloat(v, 'f', -1, 64)
}

// ParseGeoJSON decodes an RFC 7946 GeoJSON geometry object. Feature and
// FeatureCollection objects are not geometries and are rejected.
func ParseGeoJSON(data []byte) (Geometry, error) {
	var raw struct {
		Type        string            `json:"type"`
		Coordinates json.RawMessage   `json:"coordinates"`
		Geometries  []json.RawMessage `json:"geometries"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("invalid GeoJSON: %v", err)
	}
	return parseGeoJSONObj(raw.Type, raw.Coordinates, raw.Geometries)
}

func parseGeoJSONObj(typ string, coords json.RawMessage, geoms []json.RawMessage) (Geometry, error) {
	switch typ {
	case "Point":
		var c []float64
		if err := json.Unmarshal(coords, &c); err != nil {
			return nil, fmt.Errorf("invalid GeoJSON Point: %v", err)
		}
		if len(c) == 0 {
			return Point{IsEmpty: true}, nil
		}
		if len(c) < 2 {
			return nil, fmt.Errorf("invalid GeoJSON Point: need 2 coordinates")
		}
		return Point{X: c[0], Y: c[1]}, nil
	case "LineString":
		seq, err := parseCoordSeq(coords)
		if err != nil {
			return nil, fmt.Errorf("invalid GeoJSON LineString: %v", err)
		}
		return LineString{Points: seq}, nil
	case "Polygon":
		rings, err := parseRings(coords)
		if err != nil {
			return nil, fmt.Errorf("invalid GeoJSON Polygon: %v", err)
		}
		return Polygon{Rings: rings}, nil
	case "MultiPoint":
		seq, err := parseCoordSeq(coords)
		if err != nil {
			return nil, fmt.Errorf("invalid GeoJSON MultiPoint: %v", err)
		}
		pts := make([]Point, len(seq))
		for i, c := range seq {
			pts[i] = Point{X: c.X, Y: c.Y}
		}
		return MultiPoint{Points: pts}, nil
	case "MultiLineString":
		rings, err := parseRings(coords)
		if err != nil {
			return nil, fmt.Errorf("invalid GeoJSON MultiLineString: %v", err)
		}
		lines := make([]LineString, len(rings))
		for i, r := range rings {
			lines[i] = LineString{Points: r}
		}
		return MultiLineString{Lines: lines}, nil
	case "MultiPolygon":
		var rawPolys []json.RawMessage
		if err := json.Unmarshal(coords, &rawPolys); err != nil {
			return nil, fmt.Errorf("invalid GeoJSON MultiPolygon: %v", err)
		}
		polys := make([]Polygon, len(rawPolys))
		for i, rp := range rawPolys {
			rings, err := parseRings(rp)
			if err != nil {
				return nil, fmt.Errorf("invalid GeoJSON MultiPolygon: %v", err)
			}
			polys[i] = Polygon{Rings: rings}
		}
		return MultiPolygon{Polygons: polys}, nil
	case "GeometryCollection":
		subs := make([]Geometry, 0, len(geoms))
		for _, rg := range geoms {
			var sub struct {
				Type        string            `json:"type"`
				Coordinates json.RawMessage   `json:"coordinates"`
				Geometries  []json.RawMessage `json:"geometries"`
			}
			if err := json.Unmarshal(rg, &sub); err != nil {
				return nil, fmt.Errorf("invalid GeoJSON GeometryCollection: %v", err)
			}
			g, err := parseGeoJSONObj(sub.Type, sub.Coordinates, sub.Geometries)
			if err != nil {
				return nil, err
			}
			subs = append(subs, g)
		}
		return GeometryCollection{Geometries: subs}, nil
	case "":
		return nil, fmt.Errorf("invalid GeoJSON: missing type")
	default:
		return nil, fmt.Errorf("unsupported GeoJSON type %q", typ)
	}
}

func parseCoordSeq(data json.RawMessage) ([]Coord, error) {
	var rows [][]float64
	if err := json.Unmarshal(data, &rows); err != nil {
		return nil, err
	}
	out := make([]Coord, len(rows))
	for i, r := range rows {
		if len(r) < 2 {
			return nil, fmt.Errorf("need 2 coordinates per position")
		}
		out[i] = Coord{X: r[0], Y: r[1]}
	}
	return out, nil
}

func parseRings(data json.RawMessage) ([][]Coord, error) {
	var raw []json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, err
	}
	rings := make([][]Coord, len(raw))
	for i, r := range raw {
		seq, err := parseCoordSeq(r)
		if err != nil {
			return nil, err
		}
		rings[i] = seq
	}
	return rings, nil
}
