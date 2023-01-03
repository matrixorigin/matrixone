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

package trace

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/util/export/observability"
	"sync"
	"time"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/util/export/table"

	v11 "go.opentelemetry.io/proto/otlp/common/v1"
	v1 "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
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
	Status       *tracepb.Status
	Resource     *v1.Resource
	Attributes   []*v11.KeyValue
	Links        []*tracepb.Span_Link
	Events       []*tracepb.Span_Event
}

var spanPool = &sync.Pool{New: func() any {
	return &Span{}
}}

func NewSpan() *Span {
	return spanPool.Get().(*Span)
}

func (*Span) GetName() string {
	return observability.SpansTable.GetIdentify()
}

func (*Span) GetRow() *table.Row {
	return observability.SpansTable.GetRow(context.Background())
}

func (s *Span) CsvFields(ctx context.Context, row *table.Row) []string {
	row.Reset()
	row.SetColumnVal(observability.SpansTraceIDCol, s.TraceId)
	row.SetColumnVal(observability.SpansSpanIDCol, s.SpanId)
	row.SetColumnVal(observability.SpansParentTraceIDCol, s.ParentSpanId)
	row.SetColumnVal(observability.SpansSpanKindCol, s.Kind)
	row.SetColumnVal(observability.SpansSpanNameCol, s.Name)
	row.SetColumnVal(observability.SpansStartTimeCol, observability.Time2DatetimeString(s.StartTime))
	row.SetColumnVal(observability.SpansEndTimeCol, observability.Time2DatetimeString(s.EndTime))
	row.SetColumnVal(observability.SpansDurationCol, fmt.Sprintf("%d", s.Duration))
	row.SetColumnVal(observability.SpansStatusCol, trsfStatus(s.Status))
	row.SetColumnVal(observability.SpansResourceCol, trsfResource(s.Resource))
	row.SetColumnVal(observability.SpansAttributesCol, trsfAttributes(s.Attributes))
	row.SetColumnVal(observability.SpansLinksCol, trsfSpanLink(s.Links))
	row.SetColumnVal(observability.SpansEventsCol, trsfEvents(s.Events))

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

func trsfResource(r *v1.Resource) string {
	return trsfAttributes(r.Attributes)
}

func trsfAttributes(kvs []*v11.KeyValue) string {

	obj := TransferAttributes(kvs)
	bytes, err := json.Marshal(&obj)
	if err != nil {
		return "{}"
	}
	return string(bytes[:])
}

func trsfStatus(s *tracepb.Status) string {
	if bytes, err := json.Marshal(s); err != nil {
		return "{}"
	} else {
		return string(bytes[:])
	}
}

func trsfSpanLink(links []*tracepb.Span_Link) string {
	if bytes, err := json.Marshal(&links); err != nil {
		return "[]"
	} else {
		return string(bytes[:])
	}
}

func trsfEvents(events []*tracepb.Span_Event) string {
	if bytes, err := json.Marshal(&events); err != nil {
		return "[]"
	} else {
		return string(bytes[:])
	}
}

func TransferAttributes(attrs []*v11.KeyValue) map[string]any {

	m := make(map[string]any, len(attrs))
	for _, kv := range attrs {

		key := kv.GetKey()
		val := kv.GetValue()
		switch val.Value.(type) {
		case *v11.AnyValue_StringValue:
			m[key] = val.GetStringValue()
		case *v11.AnyValue_BoolValue:
			m[key] = val.GetBoolValue()
		case *v11.AnyValue_IntValue:
			m[key] = val.GetIntValue()
		case *v11.AnyValue_DoubleValue:
			m[key] = val.GetDoubleValue()
		case *v11.AnyValue_BytesValue:
			m[key] = hex.EncodeToString(val.GetBytesValue())
		case *v11.AnyValue_ArrayValue:
			m[key] = TransferArrayValue(val.GetArrayValue())
		case *v11.AnyValue_KvlistValue:
			m[key] = TransferAttributes(val.GetKvlistValue().GetValues())
		default:
			m[key] = val
		}
	}
	return m
}

func TransferArrayValue(vals *v11.ArrayValue) []any {
	if len(vals.Values) == 0 {
		return nil
	}

	val := vals.Values[0]
	arr := make([]any, 0, len(vals.Values))

	switch val.Value.(type) {
	case *v11.AnyValue_StringValue:
		for _, v := range vals.Values {
			arr = append(arr, v.GetStringValue())
		}
	case *v11.AnyValue_BoolValue:
		for _, v := range vals.Values {
			arr = append(arr, v.GetBoolValue())
		}
	case *v11.AnyValue_IntValue:
		for _, v := range vals.Values {
			arr = append(arr, v.GetIntValue())
		}
	case *v11.AnyValue_DoubleValue:
		for _, v := range vals.Values {
			arr = append(arr, v.GetDoubleValue())
		}
	default:
		for _, v := range vals.Values {
			arr = append(arr, v.GetValue())
		}
	}
	return arr
}
