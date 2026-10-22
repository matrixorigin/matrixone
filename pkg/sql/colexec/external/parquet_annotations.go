// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package external

import (
	"context"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/deprecated"
	"github.com/parquet-go/parquet-go/format"
)

type parquetAnnotationKey struct{}

// parquet-go v0.25.1 loses converted-only LIST/MAP annotations on groups.
// Preserve the actual footer annotations rather than infer types from names.
func parquetGroupAnnotations(f *parquet.File) map[*parquet.Column]*format.LogicalType {
	out := make(map[*parquet.Column]*format.LogicalType)
	index := 0
	var visit func(*parquet.Column)
	visit = func(c *parquet.Column) {
		s := f.Metadata().Schema[index]
		index++
		if !c.Leaf() {
			lt := s.LogicalType
			if lt == nil && s.ConvertedType != nil {
				switch *s.ConvertedType {
				case deprecated.List:
					lt = &format.LogicalType{List: &format.ListType{}}
				case deprecated.Map:
					lt = &format.LogicalType{Map: &format.MapType{}}
				}
			}
			if lt != nil {
				out[c] = lt
			}
		}
		for _, child := range c.Columns() {
			visit(child)
		}
	}
	visit(f.Root())
	return out
}
func withParquetAnnotations(ctx context.Context, annotations map[*parquet.Column]*format.LogicalType) context.Context {
	return context.WithValue(ctx, parquetAnnotationKey{}, annotations)
}
func parquetLogicalType(ctx context.Context, c *parquet.Column) *format.LogicalType {
	if annotations, ok := ctx.Value(parquetAnnotationKey{}).(map[*parquet.Column]*format.LogicalType); ok {
		if lt := annotations[c]; lt != nil {
			return lt
		}
	}
	return c.Type().LogicalType()
}
