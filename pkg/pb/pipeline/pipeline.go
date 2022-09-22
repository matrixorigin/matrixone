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

package pipeline

import (
	errors2 "errors"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

const MessageEnd = 1

func (m *Message) Size() int {
	return m.ProtoSize()
}

func (m *Message) GetID() uint64 {
	return m.Id
}

func (m *Message) SetID(id uint64) {
	m.Id = id
}

func (m *Message) DebugString() string {
	errStr := string(m.Err)
	if m.IsMoErr {
		me := moerr.Error{}
		_ = me.UnmarshalBinary(m.Err)
		errStr = me.Error()
	}
	return fmt.Sprintf("sid: %v, cmd: %v, data: %s, err: %s", m.Sid, m.Cmd, m.Data, errStr)
}

func (m *Message) IsEndMessage() bool {
	return m.Sid == MessageEnd
}

func EncodedMessageError(err error) ([]byte, bool) {
	var errData []byte
	var isMoErr = false
	if err == nil {
		return nil, false
	}
	if me, ok := err.(*moerr.Error); ok {
		if ed, err1 := me.MarshalBinary(); err1 == nil {
			isMoErr = true
			errData = ed
		} else {
			errData = []byte(err1.Error())
		}
	} else {
		errData = []byte(err.Error())
	}
	return errData, isMoErr
}

func DecodeMessageError(m *Message) error {
	errData := m.GetErr()
	if len(errData) > 0 {
		if m.IsMoErr {
			err := &moerr.Error{}
			if errUnmarshal := err.UnmarshalBinary(errData); errUnmarshal != nil {
				return errUnmarshal
			}
			return err
		} else {
			// handle errors returned by calling methods provided by third-party libraries or go language official libraries.
			return errors2.New(string(errData))
		}
	}
	return nil
}
