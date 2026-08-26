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

package mongodb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"go.mongodb.org/mongo-driver/v2/bson"
)

const (
	// MaxUserQueryBytes bounds both compile-time parsing and the serialized
	// execution-plan payload. The normal MongoDB scan path is unaffected.
	MaxUserQueryBytes = 1 << 20
	// MaxUserPipelineStages is deliberately below MongoDB's server limit. It
	// protects planning, plan transport, and recursive validation as well as the
	// remote server.
	MaxUserPipelineStages = 100
	// MaxUserQueryDepth bounds recursive JSON/BSON validation independently of
	// the byte limit so adversarial nesting cannot exhaust the planner stack.
	MaxUserQueryDepth = 100
	// RedactedQueryDiagnostic is the complete diagnostic representation of a
	// statement that contains an explicit MongoDB selector. It intentionally
	// retains neither selector field names nor values.
	RedactedQueryDiagnostic = "<redacted MongoDB __mo_query statement>"
)

// RedactSQLForDiagnostics returns a diagnostic-safe SQL representation. The
// selector is user supplied and may be malformed, so this detects its marker
// case-insensitively without parsing or formatting the statement.
func RedactSQLForDiagnostics(sql string) string {
	if containsASCIIFold(sql, "__mo_query") {
		return RedactedQueryDiagnostic
	}
	return sql
}

func containsASCIIFold(text, needle string) bool {
	if len(needle) == 0 {
		return true
	}
	for i := 0; i+len(needle) <= len(text); i++ {
		matched := true
		for j := range len(needle) {
			got, want := text[i+j], needle[j]
			if got >= 'A' && got <= 'Z' {
				got += 'a' - 'A'
			}
			if got != want {
				matched = false
				break
			}
		}
		if matched {
			return true
		}
	}
	return false
}

type UserQueryKind int32

const (
	UserQueryInvalid UserQueryKind = iota
	UserQueryFilter
	UserQueryPipeline
)

// UserQuery is the validated in-memory form of an explicit __mo_query value.
// It is an operation over the collection already authorized by the external
// table mapping, never an arbitrary MongoDB command.
type UserQuery struct {
	Kind     UserQueryKind
	Filter   bson.D
	Pipeline []bson.D
	Source   string
	Digest   string
}

type userQueryEnvelope struct {
	Filter   json.RawMessage `json:"filter"`
	Pipeline json.RawMessage `json:"pipeline"`
}

// ParseUserQuery parses strict JSON syntax with MongoDB Extended JSON values.
// Exactly one of filter or pipeline is accepted. Unknown JSON envelope fields,
// duplicate keys, write/cross-collection stages, server-side JavaScript, and
// operators outside the reviewed MVP subset fail closed.
func ParseUserQuery(ctx context.Context, source string) (*UserQuery, error) {
	if len(source) > MaxUserQueryBytes {
		return nil, moerr.NewInvalidInput(ctx, "MongoDB __mo_query exceeds the size limit")
	}
	raw := []byte(strings.TrimSpace(source))
	if len(raw) == 0 {
		return nil, moerr.NewInvalidInput(ctx, "MongoDB __mo_query cannot be empty")
	}
	if err := validateStrictJSON(raw); err != nil {
		return nil, moerr.NewInvalidInput(ctx, "MongoDB __mo_query must be strict Extended JSON without duplicate keys")
	}

	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var envelope userQueryEnvelope
	if err := decoder.Decode(&envelope); err != nil {
		return nil, moerr.NewInvalidInput(ctx, "MongoDB __mo_query must contain only a filter or pipeline field")
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return nil, moerr.NewInvalidInput(ctx, "MongoDB __mo_query must contain exactly one JSON object")
	}
	hasFilter := len(envelope.Filter) > 0
	hasPipeline := len(envelope.Pipeline) > 0
	if hasFilter == hasPipeline {
		return nil, moerr.NewInvalidInput(ctx, "MongoDB __mo_query requires exactly one of filter or pipeline")
	}

	query := new(UserQuery)
	if hasFilter {
		filter, err := decodeExtendedJSONDocument(ctx, envelope.Filter, "filter")
		if err != nil {
			return nil, err
		}
		if err := validateMongoValue(ctx, filter); err != nil {
			return nil, err
		}
		query.Kind = UserQueryFilter
		query.Filter = filter
		return finalizeUserQuery(ctx, query)
	}

	var stages []json.RawMessage
	if err := json.Unmarshal(envelope.Pipeline, &stages); err != nil {
		return nil, moerr.NewInvalidInput(ctx, "MongoDB __mo_query pipeline must be a JSON array")
	}
	if len(stages) == 0 {
		return nil, moerr.NewInvalidInput(ctx, "MongoDB __mo_query pipeline cannot be empty")
	}
	if len(stages) > MaxUserPipelineStages {
		return nil, moerr.NewInvalidInput(ctx, "MongoDB __mo_query pipeline exceeds the stage limit")
	}
	query.Kind = UserQueryPipeline
	query.Pipeline = make([]bson.D, 0, len(stages))
	for _, rawStage := range stages {
		stage, err := decodeExtendedJSONDocument(ctx, rawStage, "pipeline stage")
		if err != nil {
			return nil, err
		}
		if err := validateUserPipelineStage(ctx, stage); err != nil {
			return nil, err
		}
		query.Pipeline = append(query.Pipeline, stage)
	}
	return finalizeUserQuery(ctx, query)
}

func userQueryDigest(source string) string {
	sum := sha256.Sum256([]byte(source))
	return hex.EncodeToString(sum[:])
}

func finalizeUserQuery(ctx context.Context, query *UserQuery) (*UserQuery, error) {
	source, err := canonicalUserQuerySource(query)
	if err != nil {
		return nil, moerr.NewInvalidInput(ctx, "MongoDB user query cannot be represented as Extended JSON")
	}
	if len(source) > MaxUserQueryBytes {
		return nil, moerr.NewInvalidInput(ctx, "MongoDB __mo_query exceeds the size limit")
	}
	query.Source = source
	query.Digest = userQueryDigest(source)
	return query, nil
}

func canonicalUserQuerySource(query *UserQuery) (string, error) {
	var builder strings.Builder
	switch query.Kind {
	case UserQueryFilter:
		filter, err := bson.MarshalExtJSON(query.Filter, false, false)
		if err != nil {
			return "", err
		}
		builder.Grow(len(filter) + len(`{"filter":}`))
		builder.WriteString(`{"filter":`)
		builder.Write(filter)
		builder.WriteByte('}')
	case UserQueryPipeline:
		builder.WriteString(`{"pipeline":[`)
		for i, stage := range query.Pipeline {
			if i > 0 {
				builder.WriteByte(',')
			}
			encoded, err := bson.MarshalExtJSON(stage, false, false)
			if err != nil {
				return "", err
			}
			builder.Write(encoded)
		}
		builder.WriteString(`]}`)
	default:
		return "", moerr.NewInvalidInputNoCtx("invalid MongoDB user query kind")
	}
	return builder.String(), nil
}

func decodeExtendedJSONDocument(ctx context.Context, raw []byte, part string) (bson.D, error) {
	var document bson.D
	if err := bson.UnmarshalExtJSON(raw, false, &document); err != nil {
		return nil, moerr.NewInvalidInputf(ctx, "MongoDB __mo_query %s must be an Extended JSON object", part)
	}
	return document, nil
}

func validateStrictJSON(raw []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := consumeJSONValue(decoder, 0); err != nil {
		return err
	}
	_, err := decoder.Token()
	if err != io.EOF {
		return moerr.NewInvalidInputNoCtx("trailing JSON value")
	}
	return nil
}

func consumeJSONValue(decoder *json.Decoder, depth int) error {
	if depth > MaxUserQueryDepth {
		return moerr.NewInvalidInputNoCtx("JSON nesting exceeds the limit")
	}
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		seen := make(map[string]struct{})
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return moerr.NewInvalidInputNoCtx("invalid JSON object key")
			}
			if _, exists := seen[key]; exists {
				return moerr.NewInvalidInputNoCtx("duplicate JSON object key")
			}
			seen[key] = struct{}{}
			if err := consumeJSONValue(decoder, depth+1); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil || end != json.Delim('}') {
			return moerr.NewInvalidInputNoCtx("unterminated JSON object")
		}
	case '[':
		for decoder.More() {
			if err := consumeJSONValue(decoder, depth+1); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil || end != json.Delim(']') {
			return moerr.NewInvalidInputNoCtx("unterminated JSON array")
		}
	default:
		return moerr.NewInvalidInputNoCtx("invalid JSON delimiter")
	}
	return nil
}

var allowedUserPipelineStages = map[string]struct{}{
	"$match": {}, "$project": {}, "$set": {}, "$addFields": {},
	"$unset": {}, "$group": {}, "$sort": {}, "$limit": {},
	"$skip": {}, "$unwind": {}, "$count": {},
}

// allowedUserQueryOperators is intentionally an allowlist. It covers the
// common read-only filter, expression, and accumulator subset needed by the
// first implementation. Adding an operator requires an explicit security and
// resource-semantics review.
var allowedUserQueryOperators = map[string]struct{}{
	// Query predicates.
	"$and": {}, "$or": {}, "$nor": {}, "$not": {},
	"$eq": {}, "$ne": {}, "$gt": {}, "$gte": {}, "$lt": {}, "$lte": {},
	"$in": {}, "$nin": {}, "$exists": {}, "$type": {}, "$regex": {},
	"$options": {}, "$all": {}, "$elemMatch": {}, "$size": {}, "$mod": {},
	"$expr": {},
	// General expressions.
	"$literal": {}, "$cond": {}, "$ifNull": {}, "$switch": {}, "$let": {},
	"$cmp": {}, "$add": {}, "$subtract": {}, "$multiply": {}, "$divide": {},
	"$abs": {}, "$ceil": {}, "$floor": {}, "$round": {}, "$trunc": {},
	"$concat": {}, "$toLower": {}, "$toUpper": {}, "$substrBytes": {},
	"$substrCP": {}, "$strLenBytes": {}, "$strLenCP": {}, "$trim": {},
	"$ltrim": {}, "$rtrim": {}, "$arrayElemAt": {}, "$concatArrays": {},
	"$filter": {}, "$map": {}, "$reduce": {}, "$slice": {}, "$isArray": {},
	"$convert": {}, "$toBool": {}, "$toDate": {}, "$toDecimal": {},
	"$toDouble": {}, "$toInt": {}, "$toLong": {}, "$toString": {},
	"$dateAdd": {}, "$dateSubtract": {}, "$dateDiff": {}, "$dateTrunc": {},
	"$dateToString": {}, "$dateFromString": {}, "$year": {}, "$month": {},
	"$dayOfMonth": {}, "$dayOfWeek": {}, "$dayOfYear": {}, "$hour": {},
	"$minute": {}, "$second": {}, "$millisecond": {},
	// Accumulators and document expressions.
	"$sum": {}, "$avg": {}, "$min": {}, "$max": {}, "$first": {}, "$last": {},
	"$push": {}, "$addToSet": {}, "$stdDevPop": {}, "$stdDevSamp": {},
	"$mergeObjects": {}, "$getField": {}, "$setField": {}, "$unsetField": {},
}

func validateUserPipelineStage(ctx context.Context, stage bson.D) error {
	if len(stage) != 1 {
		return moerr.NewInvalidInput(ctx, "each MongoDB pipeline stage must contain exactly one operator")
	}
	operator := stage[0].Key
	if _, ok := allowedUserPipelineStages[operator]; !ok {
		return moerr.NewInvalidInput(ctx, "MongoDB pipeline stage is not allowed")
	}
	value := stage[0].Value
	switch operator {
	case "$match", "$project", "$set", "$addFields", "$group", "$sort":
		if _, ok := asBSONDocument(value); !ok {
			return moerr.NewInvalidInputf(ctx, "MongoDB pipeline stage %s requires an object", operator)
		}
	case "$limit", "$skip":
		if !isNonNegativeInteger(value) {
			return moerr.NewInvalidInputf(ctx, "MongoDB pipeline stage %s requires a non-negative integer", operator)
		}
		if operator == "$limit" && isZeroInteger(value) {
			return moerr.NewInvalidInput(ctx, "MongoDB $limit requires a positive integer")
		}
	case "$count":
		name, ok := value.(string)
		if !ok || name == "" || strings.HasPrefix(name, "$") || strings.Contains(name, ".") {
			return moerr.NewInvalidInput(ctx, "MongoDB $count requires a valid output field name")
		}
	case "$unset":
		if !isStringOrStringArray(value) {
			return moerr.NewInvalidInput(ctx, "MongoDB $unset requires a field name or array of field names")
		}
	case "$unwind":
		if _, stringForm := value.(string); !stringForm {
			if _, documentForm := asBSONDocument(value); !documentForm {
				return moerr.NewInvalidInput(ctx, "MongoDB $unwind requires a field path or object")
			}
		}
	}
	return validateMongoValue(ctx, value)
}

func validateMongoValue(ctx context.Context, value any) error {
	return validateMongoValueDepth(ctx, value, 0)
}

func validateMongoValueDepth(ctx context.Context, value any, depth int) error {
	if depth > MaxUserQueryDepth {
		return moerr.NewInvalidInput(ctx, "MongoDB query nesting exceeds the depth limit")
	}
	switch typed := value.(type) {
	case bson.JavaScript, bson.CodeWithScope:
		// Extended JSON can encode BSON code without using a $where/$function
		// document key. Reject the BSON scalar types as well as the operators so
		// no future allowlisted expression can accidentally make them executable.
		return moerr.NewInvalidInput(ctx, "MongoDB server-side JavaScript is not allowed")
	case bson.D:
		seen := make(map[string]struct{}, len(typed))
		for _, element := range typed {
			if _, exists := seen[element.Key]; exists {
				return moerr.NewInvalidInput(ctx, "MongoDB query contains duplicate document keys")
			}
			seen[element.Key] = struct{}{}
			if strings.HasPrefix(element.Key, "$") {
				if _, ok := allowedUserQueryOperators[element.Key]; !ok {
					return moerr.NewInvalidInput(ctx, "MongoDB query operator is not allowed")
				}
			}
			if err := validateMongoValueDepth(ctx, element.Value, depth+1); err != nil {
				return err
			}
		}
	case bson.M:
		for key, nested := range typed {
			if strings.HasPrefix(key, "$") {
				if _, ok := allowedUserQueryOperators[key]; !ok {
					return moerr.NewInvalidInput(ctx, "MongoDB query operator is not allowed")
				}
			}
			if err := validateMongoValueDepth(ctx, nested, depth+1); err != nil {
				return err
			}
		}
	case bson.A:
		for _, nested := range typed {
			if err := validateMongoValueDepth(ctx, nested, depth+1); err != nil {
				return err
			}
		}
	case []any:
		for _, nested := range typed {
			if err := validateMongoValueDepth(ctx, nested, depth+1); err != nil {
				return err
			}
		}
	}
	return nil
}

func asBSONDocument(value any) (bson.D, bool) {
	switch typed := value.(type) {
	case bson.D:
		return typed, true
	case bson.M:
		result := make(bson.D, 0, len(typed))
		for key, nested := range typed {
			result = append(result, bson.E{Key: key, Value: nested})
		}
		return result, true
	default:
		return nil, false
	}
}

func isNonNegativeInteger(value any) bool {
	switch typed := value.(type) {
	case int32:
		return typed >= 0
	case int64:
		return typed >= 0
	default:
		return false
	}
}

func isZeroInteger(value any) bool {
	switch typed := value.(type) {
	case int32:
		return typed == 0
	case int64:
		return typed == 0
	default:
		return false
	}
}

func isStringOrStringArray(value any) bool {
	if stringValue, ok := value.(string); ok {
		return stringValue != "" && !strings.HasPrefix(stringValue, "$")
	}
	values, ok := value.(bson.A)
	if !ok {
		return false
	}
	for _, item := range values {
		value, ok := item.(string)
		if !ok || value == "" || strings.HasPrefix(value, "$") {
			return false
		}
	}
	return len(values) > 0
}

func ApplyUserQueryToPlan(ctx context.Context, query *UserQuery, target *plan.MongoScan) error {
	if target == nil {
		return moerr.NewInvalidInput(ctx, "MongoDB user query requires a scan plan")
	}
	if query == nil {
		target.UserQueryKind = int32(UserQueryInvalid)
		target.UserFilterBson = nil
		target.UserPipelineStageBson = nil
		target.UserQueryDigest = ""
		return nil
	}
	canonicalSource, err := canonicalUserQuerySource(query)
	if err != nil || !validUserQueryDigest(query.Digest) || query.Source != canonicalSource || userQueryDigest(canonicalSource) != query.Digest {
		return moerr.NewInvalidInput(ctx, "MongoDB user query digest is invalid")
	}
	var filterBSON []byte
	var pipelineStageBSON [][]byte
	switch query.Kind {
	case UserQueryFilter:
		if err := validateMongoValue(ctx, query.Filter); err != nil {
			return err
		}
		encoded, err := bson.Marshal(query.Filter)
		if err != nil {
			return moerr.NewInvalidInput(ctx, "MongoDB filter cannot be encoded")
		}
		filterBSON = encoded
	case UserQueryPipeline:
		if len(query.Pipeline) == 0 || len(query.Pipeline) > MaxUserPipelineStages {
			return moerr.NewInvalidInput(ctx, "MongoDB pipeline has an invalid stage count")
		}
		pipelineStageBSON = make([][]byte, 0, len(query.Pipeline))
		for _, stage := range query.Pipeline {
			if err := validateUserPipelineStage(ctx, stage); err != nil {
				return err
			}
			encoded, err := bson.Marshal(stage)
			if err != nil {
				return moerr.NewInvalidInput(ctx, "MongoDB pipeline stage cannot be encoded")
			}
			pipelineStageBSON = append(pipelineStageBSON, encoded)
		}
	default:
		return moerr.NewInvalidInput(ctx, "invalid MongoDB user query kind")
	}
	if userQueryBSONBytes(filterBSON, pipelineStageBSON) > MaxUserQueryBytes {
		return moerr.NewInvalidInput(ctx, "MongoDB user query plan exceeds the size limit")
	}
	target.UserQueryKind = int32(query.Kind)
	target.UserFilterBson = filterBSON
	target.UserPipelineStageBson = pipelineStageBSON
	target.UserQueryDigest = query.Digest
	return nil
}

// UserQueryFromPlan revalidates the serialized plan on the execution CN. The
// compile-time parser is not treated as a trust boundary because plans can be
// cached and shipped over the pipeline protocol.
func UserQueryFromPlan(ctx context.Context, input *plan.MongoScan) (*UserQuery, error) {
	if input == nil || input.UserQueryKind == int32(UserQueryInvalid) {
		return nil, nil
	}
	if !validUserQueryDigest(input.UserQueryDigest) || userQueryPlanBytes(input) > MaxUserQueryBytes {
		return nil, moerr.NewInvalidInput(ctx, "MongoDB user query plan has an invalid digest or size")
	}
	query := &UserQuery{Digest: input.UserQueryDigest}
	switch UserQueryKind(input.UserQueryKind) {
	case UserQueryFilter:
		if len(input.UserFilterBson) == 0 || len(input.UserPipelineStageBson) != 0 {
			return nil, moerr.NewInvalidInput(ctx, "MongoDB filter plan has an invalid shape")
		}
		if err := bson.Unmarshal(input.UserFilterBson, &query.Filter); err != nil {
			return nil, moerr.NewInvalidInput(ctx, "MongoDB filter plan contains invalid BSON")
		}
		if err := validateMongoValue(ctx, query.Filter); err != nil {
			return nil, err
		}
		query.Kind = UserQueryFilter
	case UserQueryPipeline:
		if len(input.UserFilterBson) != 0 || len(input.UserPipelineStageBson) == 0 || len(input.UserPipelineStageBson) > MaxUserPipelineStages {
			return nil, moerr.NewInvalidInput(ctx, "MongoDB pipeline plan has an invalid shape")
		}
		query.Kind = UserQueryPipeline
		query.Pipeline = make([]bson.D, 0, len(input.UserPipelineStageBson))
		for _, encoded := range input.UserPipelineStageBson {
			var stage bson.D
			if err := bson.Unmarshal(encoded, &stage); err != nil {
				return nil, moerr.NewInvalidInput(ctx, "MongoDB pipeline plan contains invalid BSON")
			}
			if err := validateUserPipelineStage(ctx, stage); err != nil {
				return nil, err
			}
			query.Pipeline = append(query.Pipeline, stage)
		}
	default:
		return nil, moerr.NewInvalidInput(ctx, "MongoDB user query plan has an invalid kind")
	}
	canonicalSource, err := canonicalUserQuerySource(query)
	if err != nil || len(canonicalSource) > MaxUserQueryBytes || userQueryDigest(canonicalSource) != query.Digest {
		return nil, moerr.NewInvalidInput(ctx, "MongoDB user query plan does not match its digest")
	}
	query.Source = canonicalSource
	return query, nil
}

func validUserQueryDigest(digest string) bool {
	if len(digest) != sha256.Size*2 || digest != strings.ToLower(digest) {
		return false
	}
	_, err := hex.DecodeString(digest)
	return err == nil
}

func userQueryPlanBytes(query *plan.MongoScan) int {
	if query == nil {
		return 0
	}
	return userQueryBSONBytes(query.UserFilterBson, query.UserPipelineStageBson)
}

func userQueryBSONBytes(filter []byte, stages [][]byte) int {
	total := len(filter)
	for _, stage := range stages {
		total += len(stage)
	}
	return total
}

// CombineFilters intersects a user filter with the existing conservative
// automatic candidate predicate. Both remain subject to MO residual checking.
func CombineFilters(user, automatic bson.D) bson.D {
	if len(user) == 0 {
		return automatic
	}
	if len(automatic) == 0 {
		return user
	}
	return bson.D{{Key: "$and", Value: bson.A{user, automatic}}}
}
