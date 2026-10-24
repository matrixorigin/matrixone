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

package mysql

import (
	"fmt"
	"strings"
	"testing"
)

// Benchmark shapes for #30747 review: the masking fast path must keep
// keyword-bearing ORDINARY SQL (a credential word inside a string VALUE of a
// SELECT or a bulk INSERT) as close to plain-DML cost as possible — those
// statements pass through the trace/record masker for every query.

var benchSelectWithKeywordValue = "select id, note from t where note = 'credentials are here' and id = 42"

var benchSelectNoKeyword = "select id, note from t where note = 'hello world' and id = 42"

var benchBulkInsertWithKeywordValue = func() string {
	var b strings.Builder
	b.WriteString("insert into docs(id, note) values ")
	for i := 0; i < 1000; i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, "(%d, 'credentials note %d')", i, i)
	}
	return b.String()
}()

var benchBulkInsertNoKeyword = func() string {
	var b strings.Builder
	b.WriteString("insert into docs(id, note) values ")
	for i := 0; i < 1000; i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, "(%d, 'ordinary note %d')", i, i)
	}
	return b.String()
}()

var benchStageCreate = "create stage s url='s3://bucket/path' credentials={'aws_key_id'='AKIAIOSFODNN7EXAMPLE','aws_secret_key'='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'}"

var benchPrepareStage = "prepare p from 'create stage s credentials={''aws_key_id''=''AKIAIOSFODNN7EXAMPLE''}'"

func BenchmarkMaskSelectWithKeywordValue(b *testing.B) {
	for i := 0; i < b.N; i++ {
		MaskSensitiveSqlLiterals(benchSelectWithKeywordValue)
	}
}

func BenchmarkMaskSelectNoKeyword(b *testing.B) {
	for i := 0; i < b.N; i++ {
		MaskSensitiveSqlLiterals(benchSelectNoKeyword)
	}
}

func BenchmarkMaskBulkInsertWithKeywordValue(b *testing.B) {
	for i := 0; i < b.N; i++ {
		MaskSensitiveSqlLiterals(benchBulkInsertWithKeywordValue)
	}
}

func BenchmarkMaskBulkInsertNoKeyword(b *testing.B) {
	for i := 0; i < b.N; i++ {
		MaskSensitiveSqlLiterals(benchBulkInsertNoKeyword)
	}
}

func BenchmarkMaskStageCreate(b *testing.B) {
	for i := 0; i < b.N; i++ {
		MaskSensitiveSqlLiterals(benchStageCreate)
	}
}

func BenchmarkMaskPrepareStage(b *testing.B) {
	for i := 0; i < b.N; i++ {
		MaskSensitiveSqlLiterals(benchPrepareStage)
	}
}
