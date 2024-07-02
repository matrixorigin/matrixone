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

func (m *Message) GetID() uint64 {
	return m.Id
}

func (m *Message) SetID(id uint64) {
	m.Id = id
}

func (m *Message) SetSid(sid Status) {
	m.Sid = sid
}

func (m *Message) SetMoError(ctx context.Context, err error) {
	m.Err = EncodedMessageError(ctx, err)
}

func (m *Message) SetAnalysis(data []byte) {
	m.Analyse = data
}

func (m *Message) TryToGetMoErr() (error, bool) {
	errData := m.GetErr()
	if len(errData) > 0 {
		err := &moerr.Error{}
		if errUnmarshal := err.UnmarshalBinary(errData); errUnmarshal != nil {
			return errUnmarshal, true
		}
		return err, true
	}
	return nil, false
}

func (m *Message) SetMessageType(cmd Method) {
	m.Cmd = cmd
}

func (m *Message) SetData(data []byte) {
	m.Data = data
}

func (m *Message) SetProcData(data []byte) {
	m.ProcInfoData = data
}

func (m *Message) DebugString() string {
	errInfo := "none"
	if len(m.Err) > 0 {
		me := &moerr.Error{}
		if err := me.UnmarshalBinary(m.Err); err != nil {
			panic(err)
		}
		errInfo = me.Error()
	}
	return fmt.Sprintf("MessageSize: %d, sid: %d, ErrInfo: %s, batchSize: %d", m.ProtoSize(), m.Sid, errInfo, len(m.Data))
}

func (m *Message) IsBatchMessage() bool {
	return m.GetCmd() == Method_BatchMessage
}

func (m *Message) IsNotifyMessage() bool {
	return m.GetCmd() == Method_PrepareDoneNotifyMessage
}

func (m *Message) IsPipelineMessage() bool {
	return m.GetCmd() == Method_PipelineMessage
}

func (m *Message) IsEndMessage() bool {
	return m.Sid == Status_MessageEnd
}

func (m *Message) WaitingNextToMerge() bool {
	return m.Sid == Status_WaitingNext
}

func (m *Message) IsLast() bool {
	return m.Sid == Status_Last
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
