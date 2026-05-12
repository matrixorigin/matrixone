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

package function

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/functionUtil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func CastGeometryToSubtype(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opBinaryBytesBytesToBytesWithErrorCheck(ivecs, result, proc, length, func(targetSubtype, payload []byte) ([]byte, error) {
		columnMetadata := strings.TrimSpace(functionUtil.QuickBytesToStr(targetSubtype))
		columnSubtype := ""
		columnSRID := uint32(0)
		columnSRIDDefined := false
		if columnMetadata != "" {
			parts := strings.Split(columnMetadata, ";")
			head := strings.TrimSpace(parts[0])
			if head != "" && !strings.HasPrefix(strings.ToUpper(head), "SRID=") {
				columnSubtype = strings.ToUpper(head)
			}
			start := 0
			if columnSubtype != "" {
				start = 1
			}
			for _, part := range parts[start:] {
				part = strings.TrimSpace(part)
				if len(part) < len("SRID=") || !strings.EqualFold(part[:5], "SRID=") {
					continue
				}
				parsed, err := strconv.ParseUint(strings.TrimSpace(part[5:]), 10, 32)
				if err == nil {
					columnSRID = uint32(parsed)
					columnSRIDDefined = true
					break
				}
			}
		}

		wkt, valueSubtype, valueSRID, valueSRIDDefined, err := validateGeometryPayload(payload, maxPointsInGeometryLimit(proc))
		if err != nil {
			return nil, err
		}
		if columnSubtype != "" && columnSubtype != "GEOMETRY" && valueSubtype != columnSubtype {
			return nil, moerr.NewInvalidInputNoCtxf("cannot store %s in %s column", valueSubtype, columnSubtype)
		}

		if !columnSRIDDefined {
			return payload, nil
		}
		if !valueSRIDDefined {
			valueSRID = 0
		}
		if valueSRID != columnSRID {
			columnLabel := columnSubtype
			if columnLabel == "" {
				columnLabel = "GEOMETRY"
			}
			return nil, moerr.NewInvalidInputNoCtxf(
				"The SRID of the geometry does not match the SRID of the column '%s'. The SRID of the geometry is %d, but the SRID of the column is %d. Consider changing the SRID of the geometry or the SRID property of the column.",
				columnLabel,
				valueSRID,
				columnSRID,
			)
		}
		if valueSRIDDefined {
			return payload, nil
		}
		return encodeGeometryPayload(wkt, columnSRID, true), nil
	}, selectList)
}

func encodeGeometryPayload(wkt string, srid uint32, sridDefined bool) []byte {
	wkt = strings.TrimSpace(wkt)
	if !sridDefined {
		return functionUtil.QuickStrToBytes(wkt)
	}
	return functionUtil.QuickStrToBytes(fmt.Sprintf("SRID=%d;%s", srid, wkt))
}

func decodeGeometryPayload(payload []byte) (wkt string, srid uint32, sridDefined bool, err error) {
	s := strings.TrimSpace(functionUtil.QuickBytesToStr(payload))
	if len(s) == 0 {
		return "", 0, false, moerr.NewInvalidInputNoCtx("invalid geometry payload")
	}
	upper := strings.ToUpper(s)
	if !strings.HasPrefix(upper, "SRID=") {
		return s, 0, false, nil
	}

	sepIdx := strings.IndexByte(s, ';')
	if sepIdx <= len("SRID=") || sepIdx == len(s)-1 {
		return "", 0, false, moerr.NewInvalidInputNoCtx("invalid geometry payload")
	}
	value := strings.TrimSpace(s[len("SRID="):sepIdx])
	parsed, parseErr := strconv.ParseUint(value, 10, 32)
	if parseErr != nil {
		return "", 0, false, moerr.NewInvalidInputNoCtx("invalid geometry payload")
	}
	wkt = strings.TrimSpace(s[sepIdx+1:])
	if wkt == "" {
		return "", 0, false, moerr.NewInvalidInputNoCtx("invalid geometry payload")
	}
	return wkt, uint32(parsed), true, nil
}

func geometryTypeNameFromText(wkt string) (string, error) {
	s := strings.TrimSpace(wkt)
	if len(s) == 0 {
		return "", moerr.NewInvalidInputNoCtx("invalid geometry payload")
	}
	upper := strings.ToUpper(s)
	if strings.HasSuffix(upper, " EMPTY") {
		typeName := strings.TrimSpace(strings.TrimSuffix(upper, " EMPTY"))
		switch typeName {
		case "POINT", "LINESTRING", "POLYGON", "MULTIPOINT", "MULTILINESTRING", "MULTIPOLYGON", "GEOMETRYCOLLECTION":
			return typeName, nil
		default:
			return "", moerr.NewInvalidInputNoCtx("invalid geometry type")
		}
	}
	openIdx := strings.IndexByte(s, '(')
	if openIdx <= 0 {
		return "", moerr.NewInvalidInputNoCtx("invalid geometry payload")
	}
	typeName := strings.TrimSpace(upper[:openIdx])
	switch typeName {
	case "POINT", "LINESTRING", "POLYGON", "MULTIPOINT", "MULTILINESTRING", "MULTIPOLYGON", "GEOMETRYCOLLECTION":
		return typeName, nil
	case "GEOMETRY":
		return typeName, nil
	default:
		return "", moerr.NewInvalidInputNoCtx("invalid geometry type")
	}
}

const defaultMaxPointsInGeometry = int64(65536)

func validateGeometryPayload(payload []byte, maxPoints int64) (wkt string, typeName string, srid uint32, sridDefined bool, err error) {
	wkt, srid, sridDefined, err = decodeGeometryPayload(payload)
	if err != nil {
		return "", "", 0, false, err
	}
	typeName, err = geometryTypeNameFromText(wkt)
	if err != nil {
		return "", "", 0, false, err
	}
	if err = validateGeometryTextForStorage(wkt, maxPoints); err != nil {
		return "", "", 0, false, err
	}
	return wkt, typeName, srid, sridDefined, nil
}

func maxPointsInGeometryLimit(proc *process.Process) int64 {
	if proc != nil && proc.GetResolveVariableFunc() != nil {
		if v, err := proc.GetResolveVariableFunc()("max_points_in_geometry", true, false); err == nil && v != nil {
			switch val := v.(type) {
			case int64:
				return val
			case int32:
				return int64(val)
			case uint64:
				return int64(val)
			case uint32:
				return int64(val)
			case int:
				return int64(val)
			case uint:
				return int64(val)
			}
		}
	}
	return defaultMaxPointsInGeometry
}

func validateGeometryTextForStorage(wkt string, maxPoints int64) error {
	if err := validateGeometryTextStructure(wkt); err != nil {
		return err
	}
	if err := validateFiniteCoordinatesInGeometryText(wkt); err != nil {
		return err
	}
	if maxPoints <= 0 {
		return nil
	}
	pointCount, err := geometryPointCountFromText(wkt)
	if err != nil {
		return err
	}
	if pointCount > maxPoints {
		return moerr.NewInvalidInputNoCtxf("geometry has %d points, which exceeds max_points_in_geometry=%d", pointCount, maxPoints)
	}
	return nil
}

func validateGeometryTextStructure(wkt string) error {
	return validateGeometryTextStructureWithDepth(wkt, 0)
}

func validateGeometryTextStructureWithDepth(wkt string, depth int) error {
	s := strings.TrimSpace(wkt)
	typeName, err := geometryTypeNameFromText(s)
	if err != nil {
		return err
	}

	if strings.EqualFold(strings.TrimSpace(s), typeName+" EMPTY") {
		return nil
	}

	openIdx := strings.IndexByte(s, '(')
	if openIdx <= 0 || s[len(s)-1] != ')' {
		return moerr.NewInvalidInputNoCtx("invalid geometry payload")
	}
	if strings.TrimSpace(strings.ToUpper(s[:openIdx])) != typeName {
		return moerr.NewInvalidInputNoCtx("invalid geometry payload")
	}

	content := strings.TrimSpace(s[openIdx+1 : len(s)-1])
	switch typeName {
	case "POINT":
		return validatePointGeometryTextContent(content)
	case "LINESTRING":
		return validateLineStringGeometryTextContent(content)
	case "POLYGON":
		return validatePolygonGeometryTextContent(content)
	case "MULTIPOINT":
		return validateMultiPointGeometryTextContent(content, depth)
	case "MULTILINESTRING":
		return validateMultiLineStringGeometryTextContent(content)
	case "MULTIPOLYGON":
		return validateMultiPolygonGeometryTextContent(content)
	case "GEOMETRYCOLLECTION":
		return validateGeometryCollectionTextContent(content, depth)
	case "GEOMETRY":
		return validateGenericGeometryTextContent(content, depth)
	default:
		return moerr.NewInvalidInputNoCtx("invalid geometry type")
	}
}

func validatePointGeometryTextContent(content string) error {
	if content == "" {
		return moerr.NewInvalidInputNoCtx("invalid geometry payload")
	}
	_, _, err := parseCoordinatePairWithError(content, "invalid geometry payload")
	return err
}

func validateLineStringGeometryTextContent(content string) error {
	points, err := splitTopLevelGeometryItemsStrict(content, "invalid geometry payload")
	if err != nil {
		return err
	}
	if len(points) < 2 {
		return moerr.NewInvalidInputNoCtx("invalid geometry payload")
	}
	for _, point := range points {
		if _, _, err := parseCoordinatePairWithError(point, "invalid geometry payload"); err != nil {
			return err
		}
	}
	return nil
}

func validatePolygonGeometryTextContent(content string) error {
	rings, err := splitTopLevelGeometryItemsStrict(content, "invalid geometry payload")
	if err != nil {
		return err
	}
	for _, ring := range rings {
		ring = strings.TrimSpace(ring)
		if len(ring) < 2 || ring[0] != '(' || ring[len(ring)-1] != ')' {
			return moerr.NewInvalidInputNoCtx("invalid geometry payload")
		}
		if err := validatePolygonRingTextContent(ring[1 : len(ring)-1]); err != nil {
			return err
		}
	}
	return nil
}

func validatePolygonRingTextContent(content string) error {
	items, err := splitTopLevelGeometryItemsStrict(content, "invalid geometry payload")
	if err != nil {
		return err
	}
	if len(items) < 3 {
		return moerr.NewInvalidInputNoCtx("invalid geometry payload")
	}

	points := make([]geometryPoint2D, 0, len(items))
	for _, item := range items {
		x, y, err := parseCoordinatePairWithError(item, "invalid geometry payload")
		if err != nil {
			return err
		}
		points = append(points, geometryPoint2D{x: x, y: y})
	}

	if len(points) > 1 && sameGeometryPoint(points[0], points[len(points)-1]) {
		points = points[:len(points)-1]
	}
	if len(points) < 3 {
		return moerr.NewInvalidInputNoCtx("invalid geometry payload")
	}
	return nil
}

func validateMultiPointGeometryTextContent(content string, depth int) error {
	if content == "" {
		return nil
	}

	items, err := splitTopLevelGeometryItemsStrict(content, "invalid geometry payload")
	if err != nil {
		return err
	}
	for _, item := range items {
		item = strings.TrimSpace(item)
		if item == "" {
			return moerr.NewInvalidInputNoCtx("invalid geometry payload")
		}
		if itemType, err := geometryTypeNameFromText(item); err == nil {
			if itemType != "POINT" {
				return moerr.NewInvalidInputNoCtx("invalid geometry payload")
			}
			if err := validateGeometryTextStructureWithDepth(item, depth+1); err != nil {
				return err
			}
			continue
		}
		if strings.HasPrefix(item, "(") {
			if len(item) < 2 || item[len(item)-1] != ')' {
				return moerr.NewInvalidInputNoCtx("invalid geometry payload")
			}
			if err := validatePointGeometryTextContent(strings.TrimSpace(item[1 : len(item)-1])); err != nil {
				return err
			}
			continue
		}
		if err := validatePointGeometryTextContent(item); err != nil {
			return err
		}
	}
	return nil
}

func validateMultiLineStringGeometryTextContent(content string) error {
	if content == "" {
		return nil
	}

	items, err := splitTopLevelGeometryItemsStrict(content, "invalid geometry payload")
	if err != nil {
		return err
	}
	for _, item := range items {
		item = strings.TrimSpace(item)
		if len(item) < 2 || item[0] != '(' || item[len(item)-1] != ')' {
			return moerr.NewInvalidInputNoCtx("invalid geometry payload")
		}
		if err := validateLineStringGeometryTextContent(strings.TrimSpace(item[1 : len(item)-1])); err != nil {
			return err
		}
	}
	return nil
}

func validateMultiPolygonGeometryTextContent(content string) error {
	if content == "" {
		return nil
	}

	items, err := splitTopLevelGeometryItemsStrict(content, "invalid geometry payload")
	if err != nil {
		return err
	}
	for _, item := range items {
		item = strings.TrimSpace(item)
		if len(item) < 2 || item[0] != '(' || item[len(item)-1] != ')' {
			return moerr.NewInvalidInputNoCtx("invalid geometry payload")
		}
		if err := validatePolygonGeometryTextContent(strings.TrimSpace(item[1 : len(item)-1])); err != nil {
			return err
		}
	}
	return nil
}

const maxGeometryCollectionNestingDepth = 64

func validateGeometryCollectionTextContent(content string, depth int) error {
	if depth >= maxGeometryCollectionNestingDepth {
		return moerr.NewInvalidInputNoCtxf("geometry collection nesting depth exceeds %d", maxGeometryCollectionNestingDepth)
	}
	if content == "" {
		return nil
	}

	items, err := splitTopLevelGeometryItemsStrict(content, "invalid geometry payload")
	if err != nil {
		return err
	}
	for _, item := range items {
		if err := validateGeometryTextStructureWithDepth(item, depth+1); err != nil {
			return err
		}
	}
	return nil
}

func validateGenericGeometryTextContent(content string, depth int) error {
	items, err := splitTopLevelGeometryItemsStrict(content, "invalid geometry payload")
	if err != nil {
		return err
	}
	if len(items) != 1 {
		return moerr.NewInvalidInputNoCtx("invalid geometry payload")
	}
	return validateGeometryTextStructureWithDepth(items[0], depth+1)
}

func validateFiniteCoordinatesInGeometryText(wkt string) error {
	tokens := strings.FieldsFunc(wkt, func(r rune) bool {
		switch r {
		case '(', ')', ',', ' ', '\t', '\n', '\r':
			return true
		default:
			return false
		}
	})
	for _, token := range tokens {
		if token == "" {
			continue
		}
		value, err := strconv.ParseFloat(token, 64)
		if !math.IsNaN(value) && !math.IsInf(value, 0) {
			continue
		}
		if err == nil || math.IsNaN(value) || math.IsInf(value, 0) {
			return moerr.NewInvalidInputNoCtx("invalid geometry payload")
		}
	}
	return nil
}

func geometryPointCountFromText(wkt string) (int64, error) {
	return geometryPointCountFromTextWithDepth(wkt, 0)
}

func geometryPointCountFromTextWithDepth(wkt string, depth int) (int64, error) {
	s := strings.TrimSpace(wkt)
	typeName, err := geometryTypeNameFromText(s)
	if err != nil {
		return 0, err
	}
	if strings.EqualFold(strings.TrimSpace(s), typeName+" EMPTY") {
		return 0, nil
	}

	openIdx := strings.IndexByte(s, '(')
	if openIdx <= 0 || s[len(s)-1] != ')' {
		return 0, moerr.NewInvalidInputNoCtx("invalid geometry payload")
	}
	if strings.TrimSpace(strings.ToUpper(s[:openIdx])) != typeName {
		return 0, moerr.NewInvalidInputNoCtx("invalid geometry payload")
	}

	content := strings.TrimSpace(s[openIdx+1 : len(s)-1])
	switch typeName {
	case "POINT":
		if content == "" {
			return 0, moerr.NewInvalidInputNoCtx("invalid geometry payload")
		}
		return 1, nil
	case "LINESTRING":
		return coordinateListPointCount(content)
	case "POLYGON":
		return polygonPointCountFromTextContent(content)
	case "MULTIPOINT":
		return multiPointCountFromTextContent(content, depth)
	case "MULTILINESTRING":
		return multiLineStringPointCountFromTextContent(content)
	case "MULTIPOLYGON":
		return multiPolygonPointCountFromTextContent(content)
	case "GEOMETRYCOLLECTION":
		return geometryCollectionPointCountFromTextContent(content, depth)
	case "GEOMETRY":
		return genericGeometryPointCountFromTextContent(content, depth)
	default:
		return 0, moerr.NewInvalidInputNoCtx("invalid geometry type")
	}
}

func coordinateListPointCount(content string) (int64, error) {
	items, err := splitTopLevelGeometryItemsStrict(content, "invalid geometry payload")
	if err != nil {
		return 0, err
	}
	return int64(len(items)), nil
}

func polygonPointCountFromTextContent(content string) (int64, error) {
	rings, err := splitTopLevelGeometryItemsStrict(content, "invalid geometry payload")
	if err != nil {
		return 0, err
	}
	total := int64(0)
	for _, ring := range rings {
		ring = strings.TrimSpace(ring)
		if len(ring) < 2 || ring[0] != '(' || ring[len(ring)-1] != ')' {
			return 0, moerr.NewInvalidInputNoCtx("invalid geometry payload")
		}
		count, err := coordinateListPointCount(strings.TrimSpace(ring[1 : len(ring)-1]))
		if err != nil {
			return 0, err
		}
		total += count
	}
	return total, nil
}

func multiPointCountFromTextContent(content string, depth int) (int64, error) {
	if content == "" {
		return 0, nil
	}
	items, err := splitTopLevelGeometryItemsStrict(content, "invalid geometry payload")
	if err != nil {
		return 0, err
	}
	total := int64(0)
	for _, item := range items {
		item = strings.TrimSpace(item)
		if itemType, err := geometryTypeNameFromText(item); err == nil {
			if itemType != "POINT" {
				return 0, moerr.NewInvalidInputNoCtx("invalid geometry payload")
			}
			count, err := geometryPointCountFromTextWithDepth(item, depth+1)
			if err != nil {
				return 0, err
			}
			total += count
			continue
		}
		total++
	}
	return total, nil
}

func multiLineStringPointCountFromTextContent(content string) (int64, error) {
	if content == "" {
		return 0, nil
	}
	items, err := splitTopLevelGeometryItemsStrict(content, "invalid geometry payload")
	if err != nil {
		return 0, err
	}
	total := int64(0)
	for _, item := range items {
		item = strings.TrimSpace(item)
		if len(item) < 2 || item[0] != '(' || item[len(item)-1] != ')' {
			return 0, moerr.NewInvalidInputNoCtx("invalid geometry payload")
		}
		count, err := coordinateListPointCount(strings.TrimSpace(item[1 : len(item)-1]))
		if err != nil {
			return 0, err
		}
		total += count
	}
	return total, nil
}

func multiPolygonPointCountFromTextContent(content string) (int64, error) {
	if content == "" {
		return 0, nil
	}
	items, err := splitTopLevelGeometryItemsStrict(content, "invalid geometry payload")
	if err != nil {
		return 0, err
	}
	total := int64(0)
	for _, item := range items {
		item = strings.TrimSpace(item)
		if len(item) < 2 || item[0] != '(' || item[len(item)-1] != ')' {
			return 0, moerr.NewInvalidInputNoCtx("invalid geometry payload")
		}
		count, err := polygonPointCountFromTextContent(strings.TrimSpace(item[1 : len(item)-1]))
		if err != nil {
			return 0, err
		}
		total += count
	}
	return total, nil
}

func geometryCollectionPointCountFromTextContent(content string, depth int) (int64, error) {
	if content == "" {
		return 0, nil
	}
	items, err := splitTopLevelGeometryItemsStrict(content, "invalid geometry payload")
	if err != nil {
		return 0, err
	}
	total := int64(0)
	for _, item := range items {
		count, err := geometryPointCountFromTextWithDepth(item, depth+1)
		if err != nil {
			return 0, err
		}
		total += count
	}
	return total, nil
}

func genericGeometryPointCountFromTextContent(content string, depth int) (int64, error) {
	items, err := splitTopLevelGeometryItemsStrict(content, "invalid geometry payload")
	if err != nil {
		return 0, err
	}
	if len(items) != 1 {
		return 0, moerr.NewInvalidInputNoCtx("invalid geometry payload")
	}
	return geometryPointCountFromTextWithDepth(items[0], depth+1)
}

func splitTopLevelGeometryItemsStrict(content string, errMsg string) ([]string, error) {
	items := make([]string, 0)
	depth := 0
	start := 0
	for i, r := range content {
		switch r {
		case '(':
			depth++
		case ')':
			depth--
			if depth < 0 {
				return nil, moerr.NewInvalidInputNoCtx(errMsg)
			}
		case ',':
			if depth == 0 {
				item := strings.TrimSpace(content[start:i])
				if item == "" {
					return nil, moerr.NewInvalidInputNoCtx(errMsg)
				}
				items = append(items, item)
				start = i + 1
			}
		}
	}
	if depth != 0 {
		return nil, moerr.NewInvalidInputNoCtx(errMsg)
	}
	last := strings.TrimSpace(content[start:])
	if last == "" {
		return nil, moerr.NewInvalidInputNoCtx(errMsg)
	}
	items = append(items, last)
	return items, nil
}

func parseFiniteCoordinate(token string, errMsg string) (float64, error) {
	value, err := strconv.ParseFloat(token, 64)
	if err != nil || math.IsNaN(value) || math.IsInf(value, 0) {
		return 0, moerr.NewInvalidInputNoCtx(errMsg)
	}
	return value, nil
}

func parseCoordinatePairWithError(point string, errMsg string) (float64, float64, error) {
	coords := strings.Fields(strings.TrimSpace(point))
	if len(coords) != 2 {
		return 0, 0, moerr.NewInvalidInputNoCtx(errMsg)
	}

	x, err := parseFiniteCoordinate(coords[0], errMsg)
	if err != nil {
		return 0, 0, moerr.NewInvalidInputNoCtx(errMsg)
	}
	y, err := parseFiniteCoordinate(coords[1], errMsg)
	if err != nil {
		return 0, 0, moerr.NewInvalidInputNoCtx(errMsg)
	}
	return x, y, nil
}

type geometryPoint2D struct {
	x float64
	y float64
}

func sameGeometryPoint(a, b geometryPoint2D) bool {
	const epsilon = 1e-9
	return math.Abs(a.x-b.x) <= epsilon && math.Abs(a.y-b.y) <= epsilon
}
