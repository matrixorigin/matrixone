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

package plan

import "github.com/gogo/protobuf/proto"

// CopyDefaultInto copies src into a fresh, exclusively owned destination.
// The destination must not alias src or an existing published object.
func CopyDefaultInto(dst, src *Default) *Default {
	if src == nil {
		return nil
	}
	*dst = Default{OriginString: src.OriginString, NullAbility: src.NullAbility}
	if src.XXX_unrecognized != nil {
		dst.XXX_unrecognized = append([]byte{}, src.XXX_unrecognized...)
	}
	if src.Expr != nil {
		dst.Expr = proto.Clone(src.Expr).(*Expr)
	}
	return dst
}
