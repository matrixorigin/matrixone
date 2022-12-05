// Copyright 2022 Matrix Origin
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

package containers

func NewVector[T any](opts ...Options) *Vector[T] {
	var v T
	_, ok := any(v).([]byte)
	if !ok {
		return &Vector[T]{
			Vector: NewStdVector[T](opts...),
		}
	}
	return &Vector[T]{
		Vector: NewStrVector[T](opts...),
	}
}
