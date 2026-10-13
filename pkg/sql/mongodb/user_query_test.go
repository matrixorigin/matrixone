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

package mongodb

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestParseUserQueryFilterAndPlanRoundTrip(t *testing.T) {
	ctx := context.Background()
	source := `{"filter":{"ts":{"$gte":{"$date":"2026-07-27T10:00:00Z"}},"device_id":"pump-1"}}`
	query, err := ParseUserQuery(ctx, source)
	require.NoError(t, err)
	require.Equal(t, UserQueryFilter, query.Kind)
	require.Equal(t, source, query.Source)
	require.Len(t, query.Digest, 64)
	require.Len(t, query.Filter, 2)

	encoded := new(plan.MongoScan)
	require.NoError(t, ApplyUserQueryToPlan(ctx, query, encoded))
	restored, err := UserQueryFromPlan(ctx, encoded)
	require.NoError(t, err)
	require.Equal(t, query.Kind, restored.Kind)
	require.Equal(t, query.Filter, restored.Filter)
	require.Equal(t, source, restored.Source)
	require.Equal(t, query.Digest, restored.Digest)
}

func TestParseUserQueryPipelineAndPlanRoundTrip(t *testing.T) {
	ctx := context.Background()
	source := `{"pipeline":[{"$match":{"measurement":{"$type":"double"}}},{"$group":{"_id":"$device_id","event_count":{"$sum":1},"avg_measurement":{"$avg":"$measurement"}}},{"$project":{"_id":0,"device_id":"$_id","event_count":1,"avg_measurement":1}}]}`
	query, err := ParseUserQuery(ctx, source)
	require.NoError(t, err)
	require.Equal(t, UserQueryPipeline, query.Kind)
	require.Len(t, query.Pipeline, 3)

	encoded := new(plan.MongoScan)
	require.NoError(t, ApplyUserQueryToPlan(ctx, query, encoded))
	require.Equal(t, int32(UserQueryPipeline), encoded.UserQueryKind)
	require.Len(t, encoded.UserPipelineStageBson, 3)
	restored, err := UserQueryFromPlan(ctx, encoded)
	require.NoError(t, err)
	require.Equal(t, query.Pipeline, restored.Pipeline)
	require.Equal(t, source, restored.Source)
}

func TestParseUserQueryAcceptsSortAndUnwind(t *testing.T) {
	maxDepthPath := strings.Join(makeFieldPath(MaxUserFieldPathSegments), ".")
	for _, source := range []string{
		`{"pipeline":[{"$sort":{"site_id":1}}]}`,
		`{"pipeline":[{"$sort":{"` + maxDepthPath + `":1}}]}`,
		`{"pipeline":[{"$unwind":"$site_id"}]}`,
		`{"pipeline":[{"$unwind":{"path":"$site_id","includeArrayIndex":"index","preserveNullAndEmptyArrays":true}}]}`,
		`{"pipeline":[{"$unwind":"$` + maxDepthPath + `"}]}`,
	} {
		query, err := ParseUserQuery(t.Context(), source)
		require.NoError(t, err, source)
		require.Equal(t, UserQueryPipeline, query.Kind, source)
		require.Equal(t, source, query.Source, source)
		require.Len(t, query.Pipeline, 1, source)

		encoded := new(plan.MongoScan)
		require.NoError(t, ApplyUserQueryToPlan(t.Context(), query, encoded), source)
		restored, err := UserQueryFromPlan(t.Context(), encoded)
		require.NoError(t, err, source)
		require.Equal(t, query.Pipeline, restored.Pipeline, source)
	}

	sortFields := make([]string, MaxUserSortKeys)
	for i := range sortFields {
		sortFields[i] = fmt.Sprintf(`"field_%d":1`, i)
	}
	_, err := ParseUserQuery(t.Context(), `{"pipeline":[{"$sort":{`+strings.Join(sortFields, ",")+`}}]}`)
	require.NoError(t, err)
}

func TestRedactSQLForDiagnostics(t *testing.T) {
	for _, sql := range []string{
		`select * from t where __mo_query = '{"filter":{"password":"super-secret-value"}}'`,
		`select * from t where __MO_QUERY = '{"pipeline":[{"$match":{"api_key":"super-secret-value"}}]}'`,
	} {
		diagnostic := RedactSQLForDiagnostics(sql)
		require.Equal(t, RedactedQueryDiagnostic, diagnostic)
		require.NotContains(t, diagnostic, "password")
		require.NotContains(t, diagnostic, "api_key")
		require.NotContains(t, diagnostic, "super-secret-value")
	}
	require.Equal(t, "select 1", RedactSQLForDiagnostics("select 1"))
}

func TestParseUserQueryRejectsMalformedAndAmbiguousInput(t *testing.T) {
	tests := []struct {
		name   string
		source string
		want   string
	}{
		{name: "empty", source: " ", want: "cannot be empty"},
		{name: "top level array", source: `[]`, want: "filter or pipeline"},
		{name: "unknown envelope field", source: `{"command":{}}`, want: "filter or pipeline"},
		{name: "uppercase filter envelope", source: `{"FILTER":{}}`, want: "filter or pipeline"},
		{name: "uppercase pipeline envelope", source: `{"PIPELINE":[{"$match":{}}]}`, want: "filter or pipeline"},
		{name: "case variant duplicate envelope", source: `{"filter":{},"Filter":{"value":1}}`, want: "filter or pipeline"},
		{name: "no operation", source: `{}`, want: "exactly one"},
		{name: "both operations", source: `{"filter":{},"pipeline":[{"$match":{}}]}`, want: "exactly one"},
		{name: "duplicate envelope field", source: `{"filter":{},"filter":{}}`, want: "strict Extended JSON"},
		{name: "duplicate nested field", source: `{"filter":{"device_id":"a","device_id":"b"}}`, want: "strict Extended JSON"},
		{name: "trailing value", source: `{"filter":{}} {}`, want: "strict Extended JSON"},
		{name: "filter is array", source: `{"filter":[]}`, want: "Extended JSON object"},
		{name: "pipeline is object", source: `{"pipeline":{}}`, want: "JSON array"},
		{name: "empty pipeline", source: `{"pipeline":[]}`, want: "cannot be empty"},
		{name: "stage is array", source: `{"pipeline":[[]]}`, want: "Extended JSON object"},
		{name: "stage has two operators", source: `{"pipeline":[{"$match":{},"$limit":1}]}`, want: "exactly one operator"},
		{name: "limit zero", source: `{"pipeline":[{"$limit":0}]}`, want: "positive integer"},
		{name: "limit negative", source: `{"pipeline":[{"$limit":-1}]}`, want: "non-negative integer"},
		{name: "count field path", source: `{"pipeline":[{"$count":"a.b"}]}`, want: "valid output field"},
		{name: "unset empty", source: `{"pipeline":[{"$unset":[]}]}`, want: "field name"},
		{name: "sort is array", source: `{"pipeline":[{"$sort":[]}]}`, want: "$sort requires 1 to 32 fields"},
		{name: "sort is empty", source: `{"pipeline":[{"$sort":{}}]}`, want: "$sort requires 1 to 32 fields"},
		{name: "sort direction is zero", source: `{"pipeline":[{"$sort":{"site_id":0}}]}`, want: "1 or -1 directions"},
		{name: "sort dollar field", source: `{"pipeline":[{"$sort":{"$natural":1}}]}`, want: "$sort requires 1 to 32 fields"},
		{name: "sort empty path segment", source: `{"pipeline":[{"$sort":{"site_id..value":1}}]}`, want: "$sort requires 1 to 32 fields"},
		{name: "sort path too deep", source: `{"pipeline":[{"$sort":{"` + strings.Join(makeFieldPath(MaxUserFieldPathSegments+1), ".") + `":1}}]}`, want: "$sort requires 1 to 32 fields"},
		{name: "unwind number", source: `{"pipeline":[{"$unwind":1}]}`, want: "$unwind requires a valid field path"},
		{name: "unwind string is not path", source: `{"pipeline":[{"$unwind":"site_id"}]}`, want: "$unwind requires a valid field path"},
		{name: "unwind variable is not path", source: `{"pipeline":[{"$unwind":"$$ROOT"}]}`, want: "$unwind requires a valid field path"},
		{name: "unwind trailing dot", source: `{"pipeline":[{"$unwind":"$site_id."}]}`, want: "$unwind requires a valid field path"},
		{name: "unwind empty path segment", source: `{"pipeline":[{"$unwind":"$site_id..value"}]}`, want: "$unwind requires a valid field path"},
		{name: "unwind dollar path segment", source: `{"pipeline":[{"$unwind":"$site_id.$value"}]}`, want: "$unwind requires a valid field path"},
		{name: "unwind path too deep", source: `{"pipeline":[{"$unwind":"$` + strings.Join(makeFieldPath(MaxUserFieldPathSegments+1), ".") + `"}]}`, want: "$unwind requires a valid field path"},
		{name: "unwind object missing path", source: `{"pipeline":[{"$unwind":{"preserveNullAndEmptyArrays":true}}]}`, want: "$unwind requires a valid field path"},
		{name: "unwind object unknown option", source: `{"pipeline":[{"$unwind":{"path":"$site_id","futureOption":true}}]}`, want: "$unwind requires a valid field path"},
		{name: "unwind object invalid index", source: `{"pipeline":[{"$unwind":{"path":"$site_id","includeArrayIndex":"$index"}}]}`, want: "$unwind requires a valid field path"},
		{name: "unwind object invalid dotted index", source: `{"pipeline":[{"$unwind":{"path":"$site_id","includeArrayIndex":"index..value"}}]}`, want: "$unwind requires a valid field path"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := ParseUserQuery(t.Context(), test.source)
			require.ErrorContains(t, err, test.want)
		})
	}

	sortFields := make([]string, MaxUserSortKeys+1)
	for i := range sortFields {
		sortFields[i] = fmt.Sprintf(`"field_%d":1`, i)
	}
	_, err := ParseUserQuery(t.Context(), `{"pipeline":[{"$sort":{`+strings.Join(sortFields, ",")+`}}]}`)
	require.ErrorContains(t, err, "$sort requires 1 to 32 fields")

	_, err = ParseUserQuery(t.Context(), `{"filter":{"value":"`+strings.Repeat("x", MaxUserQueryBytes)+`"}}`)
	require.ErrorContains(t, err, "size limit")
	_, err = ParseUserQuery(t.Context(), strings.Repeat(" ", MaxUserQueryBytes+1)+`{"filter":{}}`)
	require.ErrorContains(t, err, "size limit")

	depth := MaxUserQueryDepth + 1
	_, err = ParseUserQuery(t.Context(), `{"filter":{"value":`+
		strings.Repeat("[", depth)+`0`+strings.Repeat("]", depth)+`}}`)
	require.ErrorContains(t, err, "strict Extended JSON")

	stages := strings.Repeat(`{"$match":{}},`, MaxUserPipelineStages) + `{"$match":{}}`
	_, err = ParseUserQuery(t.Context(), `{"pipeline":[`+stages+`]}`)
	require.ErrorContains(t, err, "stage limit")
}

func makeFieldPath(segments int) []string {
	path := make([]string, segments)
	for i := range path {
		path[i] = "a"
	}
	return path
}

func TestParseUserQueryRejectsUnsafeStagesAndOperators(t *testing.T) {
	for _, stage := range []string{
		`{"$out":"archive"}`,
		`{"$merge":"archive"}`,
		`{"$lookup":{"from":"other","as":"joined","pipeline":[]}}`,
		`{"$graphLookup":{"from":"other","startWith":"$id","connectFromField":"id","connectToField":"id","as":"joined"}}`,
		`{"$unionWith":"other"}`,
		`{"$collStats":{}}`,
		`{"$indexStats":{}}`,
		`{"$currentOp":{}}`,
		`{"$planCacheStats":{}}`,
		`{"$futureStage":{}}`,
	} {
		_, err := ParseUserQuery(t.Context(), `{"pipeline":[`+stage+`]}`)
		require.ErrorContains(t, err, "is not allowed", stage)
	}

	for _, source := range []string{
		`{"filter":{"$where":"this.value > 1"}}`,
		`{"filter":{"value":{"$unknown":1}}}`,
		`{"filter":{"value":{"$code":"function() { return true; }"}}}`,
		`{"filter":{"value":{"$code":"function() { return true; }","$scope":{}}}}`,
		`{"pipeline":[{"$project":{"value":{"$function":{"body":"function(){}","args":[],"lang":"js"}}}}]}`,
		`{"pipeline":[{"$group":{"_id":null,"value":{"$accumulator":{"init":"function(){}"}}}}]}`,
		`{"pipeline":[{"$group":{"_id":null,"value":{"$push":"$value"}}}]}`,
		`{"pipeline":[{"$group":{"_id":null,"value":{"$addToSet":"$value"}}}]}`,
	} {
		_, err := ParseUserQuery(t.Context(), source)
		if strings.Contains(source, `"$code"`) {
			require.ErrorContains(t, err, "server-side JavaScript", source)
		} else {
			require.ErrorContains(t, err, "is not allowed", source)
		}
		require.NotContains(t, err.Error(), "function()", "errors must not echo JavaScript or query literals")
	}
}

func TestUserQueryPlanRevalidationFailsClosed(t *testing.T) {
	ctx := context.Background()
	legacy, err := UserQueryFromPlan(ctx, &plan.MongoScan{})
	require.NoError(t, err)
	require.Nil(t, legacy)
	for name, candidate := range map[string]*plan.MongoScan{
		"filter payload":   {UserFilterBson: []byte{3, 0, 0, 0}},
		"pipeline payload": {UserPipelineStageBson: [][]byte{{3, 0, 0, 0}}},
		"digest payload":   {UserQueryDigest: strings.Repeat("a", 64)},
	} {
		t.Run("zero kind with "+name, func(t *testing.T) {
			_, err := UserQueryFromPlan(ctx, candidate)
			require.ErrorContains(t, err, "zero kind")
		})
	}

	query, err := ParseUserQuery(ctx, `{"filter":{"device_id":"pump-1"}}`)
	require.NoError(t, err)
	valid := new(plan.MongoScan)
	require.NoError(t, ApplyUserQueryToPlan(ctx, query, valid))

	tests := []struct {
		name   string
		mutate func(*plan.MongoScan)
		want   string
	}{
		{name: "invalid kind", mutate: func(q *plan.MongoScan) { q.UserQueryKind = 99 }, want: "invalid kind"},
		{name: "invalid digest", mutate: func(q *plan.MongoScan) { q.UserQueryDigest = "secret" }, want: "invalid digest or size"},
		{name: "digest mismatch", mutate: func(q *plan.MongoScan) { q.UserQueryDigest = strings.Repeat("a", 64) }, want: "does not match"},
		{name: "filter with stages", mutate: func(q *plan.MongoScan) { q.UserPipelineStageBson = [][]byte{{1}} }, want: "invalid shape"},
		{name: "invalid filter bson", mutate: func(q *plan.MongoScan) { q.UserFilterBson = []byte{1} }, want: "invalid BSON"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := *valid
			candidate.UserFilterBson = append([]byte(nil), valid.UserFilterBson...)
			candidate.UserPipelineStageBson = append([][]byte(nil), valid.UserPipelineStageBson...)
			test.mutate(&candidate)
			_, err := UserQueryFromPlan(ctx, &candidate)
			require.ErrorContains(t, err, test.want)
		})
	}
	duplicateFilter, err := bson.Marshal(bson.D{
		{Key: "device_id", Value: "pump-1"},
		{Key: "device_id", Value: "pump-2"},
	})
	require.NoError(t, err)
	duplicatePlan := *valid
	duplicatePlan.UserFilterBson = duplicateFilter
	_, err = UserQueryFromPlan(ctx, &duplicatePlan)
	require.ErrorContains(t, err, "duplicate document keys")

	pipeline, err := ParseUserQuery(ctx, `{"pipeline":[{"$match":{}}]}`)
	require.NoError(t, err)
	validPipelinePlan := new(plan.MongoScan)
	require.NoError(t, ApplyUserQueryToPlan(ctx, pipeline, validPipelinePlan))
	for _, test := range []struct {
		name  string
		stage bson.D
		want  string
	}{
		{name: "unsafe stage", stage: bson.D{{Key: "$out", Value: "archive"}}, want: "is not allowed"},
		{name: "invalid sort", stage: bson.D{{Key: "$sort", Value: bson.D{{Key: "site_id", Value: int32(0)}}}}, want: "1 or -1 directions"},
		{name: "invalid unwind", stage: bson.D{{Key: "$unwind", Value: "site_id"}}, want: "valid field path"},
	} {
		t.Run(test.name+" in execution plan", func(t *testing.T) {
			encodedStage, err := bson.Marshal(test.stage)
			require.NoError(t, err)
			candidate := *validPipelinePlan
			candidate.UserPipelineStageBson = [][]byte{encodedStage}
			_, err = UserQueryFromPlan(ctx, &candidate)
			require.ErrorContains(t, err, test.want)
		})
	}
}

func TestCombineFilters(t *testing.T) {
	user := bson.D{{Key: "device_id", Value: "pump-1"}}
	automatic := bson.D{{Key: "measurement", Value: bson.D{{Key: "$gte", Value: 10}}}}
	require.Equal(t, automatic, CombineFilters(nil, automatic))
	require.Equal(t, user, CombineFilters(user, nil))
	require.Equal(t, bson.D{{Key: "$and", Value: bson.A{user, automatic}}}, CombineFilters(user, automatic))
}
