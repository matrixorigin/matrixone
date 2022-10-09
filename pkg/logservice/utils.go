// Copyright 2021 - 2022 Matrix Origin
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

package logservice

type Marshaller interface {
	Marshal() ([]byte, error)
}

type Unmarshaler interface {
	Unmarshal([]byte) error
}

func MustMarshal(m Marshaller) []byte {
	data, err := m.Marshal()
	if err != nil {
		panic(err)
	}
	return data
}

func MustUnmarshal(m Unmarshaler, data []byte) {
	if err := m.Unmarshal(data); err != nil {
		panic(err)
	}
}
