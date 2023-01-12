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
	"context"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

const (
	BatchEnd = iota
	MessageEnd
	WaitingNext
)

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
	errInfo := "none"
	if len(m.Err) > 0 {
		me := moerr.Error{}
		errInfo = me.UnmarshalBinary(m.Err).Error()
	}
	return fmt.Sprintf("MessageSize: %d, sid: %d, ErrInfo: %s, batchSize: %d", m.Size(), m.Sid, errInfo, len(m.Data))
}

func (m *Message) IsEndMessage() bool {
	return m.Sid == MessageEnd
}

func (m *Message) WaitingNextToMerge() bool {
	return m.Sid == WaitingNext
}

func EncodedMessageError(ctx context.Context, err error) []byte {
	var errData []byte
	if err == nil {
		return nil
	}
	if me, ok := err.(*moerr.Error); ok {
		if ed, err1 := me.MarshalBinary(); err1 == nil {
			errData = ed
		} else {
			errData, _ = err1.(*moerr.Error).MarshalBinary()
		}
	} else {
		// XXXXX It's so bad that if we still received non mo err here. Just convert all them to be mo err now.
		// once we eliminate all the hidden dangers brought by non mo err, should delete these code.
		errData, _ = moerr.ConvertGoError(ctx, err).(*moerr.Error).MarshalBinary()
	}
	return errData
}

func GetMessageErrorInfo(m *Message) error {
	errData := m.GetErr()
	if len(errData) > 0 {
		err := &moerr.Error{}
		if errUnmarshal := err.UnmarshalBinary(errData); errUnmarshal != nil {
			return errUnmarshal
		}
		return err
	}
	return nil
}
