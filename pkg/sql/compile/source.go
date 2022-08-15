// Copyright 2021 Matrix Origin
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

package compile

import "github.com/matrixorigin/matrixone/pkg/container/types"

func (s *Source) MarshalBinary() ([]byte, error) {
	return types.Encode(&EncodeSource{
		Bat:          s.Bat,
		SchemaName:   s.SchemaName,
		RelationName: s.RelationName,
		Attributes:   s.Attributes,
	})
}

func (s *Source) UnmarshalBinary(data []byte) error {
	rs := new(EncodeSource)
	if err := types.Decode(data, rs); err != nil {
		return err
	}
	s.Bat = rs.Bat
	s.SchemaName = rs.SchemaName
	s.RelationName = rs.RelationName
	s.Attributes = rs.Attributes
	return nil
}
