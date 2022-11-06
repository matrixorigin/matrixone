// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logtail

import (
	"bytes"
	"fmt"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

func ToStringTemplate(
	vec containers.Vector,
	toStrFn func(t types.Type, v any) string) string {
	var w bytes.Buffer
	_, _ = w.WriteString(fmt.Sprintf("[%d]: ", vec.Length()))
	first := true
	for i := 0; i < vec.Length(); i++ {
		if !first {
			_ = w.WriteByte(',')
		}
		v := vec.Get(i)
		_, _ = w.WriteString(toStrFn(vec.GetType(), v))
		first = false
	}

	return w.String()
}

func SpecialRowidsToString(vec containers.Vector) string {
	return ToStringTemplate(
		vec,
		func(t types.Type, v any) string {
			val := v.(types.Rowid)
			return fmt.Sprintf("%d", types.DecodeUint64(val[:]))
		})
}

func RowidsToString(vec containers.Vector) string {
	return ToStringTemplate(
		vec,
		func(t types.Type, v any) string {
			val := v.(types.Rowid)
			blkID, offset := pkgcatalog.DecodeRowid(val)
			return fmt.Sprintf("BLK-%d:Off-%d", blkID, offset)
		})
}

func VectorToString(vec containers.Vector) string {
	return ToStringTemplate(
		vec,
		func(t types.Type, v any) string {
			return common.TypeStringValue(t, v)
		})
}

func BatchToString(name string, bat *containers.Batch, isSpecialRowID bool) string {
	var w bytes.Buffer
	_, _ = w.WriteString(fmt.Sprintf("[BatchName=%s]\n", name))
	for i, vec := range bat.Vecs {
		_, _ = w.WriteString(fmt.Sprintf("(attr=%s)", bat.Attrs[i]))
		if bat.Attrs[i] == catalog.AttrRowID {
			if isSpecialRowID {
				_, _ = w.WriteString(SpecialRowidsToString(vec))
			} else {
				_, _ = w.WriteString(RowidsToString(vec))
			}
		} else {
			_, _ = w.WriteString(VectorToString(vec))
		}
		_ = w.WriteByte('\n')
	}
	return w.String()
}
