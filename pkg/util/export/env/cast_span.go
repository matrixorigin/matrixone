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

package env

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
	v11 "go.opentelemetry.io/proto/otlp/common/v1"
	v1 "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"sync"
	"time"
	"unsafe"
)

type Span struct {
	TraceId      string
	SpanId       string
	ParentSpanId string
	Kind         string
	Name         string
	StartTime    time.Time
	EndTime      time.Time
	Duration     uint64
	Resource     *v1.Resource
	Links        []tracepb.Span_Link
	Attributes   []*v11.KeyValue
}

var spanPool = &sync.Pool{New: func() any {
	return &Span{}
}}

func NewSpan() *Span {
	return spanPool.Get().(*Span)
}

func (*Span) GetName() string {
	return SpansTable.GetIdentify()
}

func (*Span) GetRow() *table.Row {
	return SpansTable.GetRow(context.Background())
}

func (s *Span) CsvFields(ctx context.Context, row *table.Row) []string {
	row.Reset()
	row.SetColumnVal(SpansTraceIDCol, s.TraceId)
	row.SetColumnVal(SpansSpanIDCol, s.SpanId)
	row.SetColumnVal(SpansParentTraceIDCol, s.ParentSpanId)

	return row.ToStrings()
}

func (s *Span) Size() int64 {
	return int64(unsafe.Sizeof(s)) + int64(
		len(s.TraceId)+len(s.SpanId)+len(s.ParentSpanId)+len(s.Kind)+len(s.Name),
	)
}

func (s *Span) Free() {
	s.TraceId = ""
	s.SpanId = ""
	s.ParentSpanId = ""
	s.Kind = ""
	s.Name = ""
	s.StartTime = time.Time{}
	s.EndTime = time.Time{}
	s.Duration = 0
	s.Resource = nil
	s.Links = nil
	s.Attributes = nil
	spanPool.Put(s)
}
