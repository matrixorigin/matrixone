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

package types

import (
	"github.com/google/uuid"
)

func ParseUuid(str string) (Uuid, error) {
	gUuid, err := uuid.Parse(str)
	if err != nil {
		return Uuid{}, err
	}
	return Uuid(gUuid), nil
}

func BuildUuid() (Uuid, error) {
	gUuid, err := uuid.NewUUID()
	if err != nil {
		return Uuid{}, err
	}
	return Uuid(gUuid), nil
}

func UuidToString(muuid Uuid) (string, error) {
	return uuid.UUID(muuid).String(), nil
}

func EqualUuid(src Uuid, dest Uuid) bool {
	return src == dest
}

func CompareUuid(left Uuid, right Uuid) int64 {
	for i := 0; i < 16; i++ {
		if left[i] == right[i] {
			continue
		} else if left[i] > right[i] {
			return +1
		} else {
			return -1
		}
	}
	return 0
}

func (d Uuid) ToString() string {
	return uuid.UUID(d).String()
}

func (d Uuid) ClockSequence() int {
	return uuid.UUID(d).ClockSequence()
}

func (d Uuid) Compare(other Uuid) int {
	return int(CompareUuid(d, other))
}
func (d Uuid) Eq(other Uuid) bool {
	return d.Compare(other) == 0
}
func (d Uuid) Le(other Uuid) bool {
	return d.Compare(other) <= 0
}
func (d Uuid) Lt(other Uuid) bool {
	return d.Compare(other) < 0
}
func (d Uuid) Ge(other Uuid) bool {
	return d.Compare(other) >= 0
}
func (d Uuid) Gt(other Uuid) bool {
	return d.Compare(other) > 0
}
func (d Uuid) Ne(other Uuid) bool {
	return d.Compare(other) != 0
}

// ProtoSize is used by gogoproto.
func (d *Uuid) ProtoSize() int {
	return 16
}

// MarshalToSizedBuffer is used by gogoproto.
func (d *Uuid) MarshalToSizedBuffer(data []byte) (int, error) {
	if len(data) < d.ProtoSize() {
		panic("invalid byte slice")
	}
	n := copy(data, d[:])
	return n, nil
}

// MarshalTo is used by gogoproto.
func (d *Uuid) MarshalTo(data []byte) (int, error) {
	size := d.ProtoSize()
	return d.MarshalToSizedBuffer(data[:size])
}

// Marshal is used by gogoproto.
func (d *Uuid) Marshal() ([]byte, error) {
	data := make([]byte, d.ProtoSize())
	n, err := d.MarshalToSizedBuffer(data)
	if err != nil {
		return nil, err
	}
	return data[:n], err
}

// Unmarshal is used by gogoproto.
func (d *Uuid) Unmarshal(data []byte) error {
	if len(data) < d.ProtoSize() {
		panic("invalid byte slice")
	}
	copy(d[:], data)
	return nil
}
