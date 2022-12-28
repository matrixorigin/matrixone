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
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
	v11 "go.opentelemetry.io/proto/otlp/common/v1"
	v1 "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"time"
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

func Transafer(data *tracepb.TracesData) []table.Row {

}
