// Copyright 2026 Matrix Origin
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
	"encoding/json"
	"fmt"

	"github.com/gogo/protobuf/proto"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
)

// pipeline.pb.go intentionally routes the String methods for Message,
// IcebergDataFileTask, IcebergDeleteFileTask, ExternalScan, Instruction, and
// Pipeline through the helpers below. protoc regeneration restores
// proto.CompactTextString and must be followed by restoring those routes until
// the generator supports per-message redacting stringers. GoString and JSON
// methods alone are insufficient because logging commonly calls String.

func messageString(m *Message) string {
	if m == nil {
		return "<nil>"
	}
	if m.IsPipelineMessage() || m.GetCmd() == Method_PrepareDoneNotifyMessage {
		return fmt.Sprintf(
			"sid:%s cmd:%s id:%d data:<redacted:%d bytes> proc_info_data:<redacted:%d bytes> err:%d bytes analyse:%d bytes needNotReply:%t",
			m.GetSid().String(),
			m.GetCmd().String(),
			m.GetId(),
			len(m.GetData()),
			len(m.GetProcInfoData()),
			len(m.GetErr()),
			len(m.GetAnalyse()),
			m.GetNeedNotReply(),
		)
	}
	return proto.CompactTextString(m)
}

func (m *Message) GoString() string {
	return messageString(m)
}

func (m *Message) MarshalJSON() ([]byte, error) {
	if m == nil {
		return []byte("null"), nil
	}
	if m.IsPipelineMessage() || m.GetCmd() == Method_PrepareDoneNotifyMessage {
		return json.Marshal(struct {
			Sid              string `json:"sid,omitempty"`
			Cmd              string `json:"cmd,omitempty"`
			ID               uint64 `json:"id,omitempty"`
			Data             string `json:"data,omitempty"`
			ProcInfoData     string `json:"proc_info_data,omitempty"`
			ErrBytes         int    `json:"err_bytes,omitempty"`
			AnalyseBytes     int    `json:"analyse_bytes,omitempty"`
			NeedNotReply     bool   `json:"need_not_reply,omitempty"`
			DebugMsgRedacted string `json:"debug_msg,omitempty"`
		}{
			Sid:              m.GetSid().String(),
			Cmd:              m.GetCmd().String(),
			ID:               m.GetId(),
			Data:             redactedBytes(len(m.GetData())),
			ProcInfoData:     redactedBytes(len(m.GetProcInfoData())),
			ErrBytes:         len(m.GetErr()),
			AnalyseBytes:     len(m.GetAnalyse()),
			NeedNotReply:     m.GetNeedNotReply(),
			DebugMsgRedacted: redactNonEmpty(m.GetDebugMsg()),
		})
	}
	type messageAlias Message
	return json.Marshal((*messageAlias)(m))
}

func icebergDataFileTaskString(m *IcebergDataFileTask) string {
	if m == nil {
		return "<nil>"
	}
	cp := *m
	cp.FilePath = api.RedactPath(cp.FilePath)
	cp.CredentialScope = redactNonEmpty(cp.CredentialScope)
	return proto.CompactTextString(&cp)
}

func (m *IcebergDataFileTask) GoString() string {
	return icebergDataFileTaskString(m)
}

func (m *IcebergDataFileTask) MarshalJSON() ([]byte, error) {
	if m == nil {
		return []byte("null"), nil
	}
	cp := *m
	cp.FilePath = api.RedactPath(cp.FilePath)
	cp.CredentialScope = redactNonEmpty(cp.CredentialScope)
	type taskAlias IcebergDataFileTask
	return json.Marshal((*taskAlias)(&cp))
}

func icebergDeleteFileTaskString(m *IcebergDeleteFileTask) string {
	if m == nil {
		return "<nil>"
	}
	cp := *m
	cp.DeleteFilePath = api.RedactPath(cp.DeleteFilePath)
	cp.ReferencedDataFile = api.RedactPath(cp.ReferencedDataFile)
	cp.CredentialScope = redactNonEmpty(cp.CredentialScope)
	return proto.CompactTextString(&cp)
}

func (m *IcebergDeleteFileTask) GoString() string {
	return icebergDeleteFileTaskString(m)
}

func (m *IcebergDeleteFileTask) MarshalJSON() ([]byte, error) {
	if m == nil {
		return []byte("null"), nil
	}
	cp := *m
	cp.DeleteFilePath = api.RedactPath(cp.DeleteFilePath)
	cp.ReferencedDataFile = api.RedactPath(cp.ReferencedDataFile)
	cp.CredentialScope = redactNonEmpty(cp.CredentialScope)
	type taskAlias IcebergDeleteFileTask
	return json.Marshal((*taskAlias)(&cp))
}

func externalScanString(m *ExternalScan) string {
	if m == nil {
		return "<nil>"
	}
	return proto.CompactTextString(redactedExternalScanCopy(m))
}

func (m *ExternalScan) GoString() string {
	return externalScanString(m)
}

func (m *ExternalScan) MarshalJSON() ([]byte, error) {
	if m == nil {
		return []byte("null"), nil
	}
	cp := redactedExternalScanCopy(m)
	type scanAlias ExternalScan
	return json.Marshal((*scanAlias)(cp))
}

func instructionString(m *Instruction) string {
	if m == nil {
		return "<nil>"
	}
	cp := *m
	if hasIcebergRuntime(m.ExternalScan) {
		cp.ExternalScan = redactedExternalScanCopy(m.ExternalScan)
	}
	return proto.CompactTextString(&cp)
}

func (m *Instruction) GoString() string {
	return instructionString(m)
}

func (m *Instruction) MarshalJSON() ([]byte, error) {
	if m == nil {
		return []byte("null"), nil
	}
	cp := redactedInstructionCopy(m)
	type instructionAlias Instruction
	return json.Marshal((*instructionAlias)(cp))
}

func pipelineString(m *Pipeline) string {
	if m == nil {
		return "<nil>"
	}
	cp := *m
	if len(m.InstructionList) > 0 {
		cp.InstructionList = make([]*Instruction, 0, len(m.InstructionList))
		for _, instr := range m.InstructionList {
			cp.InstructionList = append(cp.InstructionList, redactedInstructionCopy(instr))
		}
	}
	if len(m.Children) > 0 {
		cp.Children = make([]*Pipeline, 0, len(m.Children))
		for _, child := range m.Children {
			cp.Children = append(cp.Children, redactedPipelineCopy(child))
		}
	}
	return proto.CompactTextString(&cp)
}

func (m *Pipeline) GoString() string {
	return pipelineString(m)
}

func (m *Pipeline) MarshalJSON() ([]byte, error) {
	if m == nil {
		return []byte("null"), nil
	}
	cp := redactedPipelineCopy(m)
	type pipelineAlias Pipeline
	return json.Marshal((*pipelineAlias)(cp))
}

func redactedPipelineCopy(m *Pipeline) *Pipeline {
	if m == nil {
		return nil
	}
	cp := *m
	if len(m.InstructionList) > 0 {
		cp.InstructionList = make([]*Instruction, 0, len(m.InstructionList))
		for _, instr := range m.InstructionList {
			cp.InstructionList = append(cp.InstructionList, redactedInstructionCopy(instr))
		}
	}
	if len(m.Children) > 0 {
		cp.Children = make([]*Pipeline, 0, len(m.Children))
		for _, child := range m.Children {
			cp.Children = append(cp.Children, redactedPipelineCopy(child))
		}
	}
	return &cp
}

func redactedInstructionCopy(m *Instruction) *Instruction {
	if m == nil {
		return nil
	}
	cp := *m
	if hasIcebergRuntime(m.ExternalScan) {
		cp.ExternalScan = redactedExternalScanCopy(m.ExternalScan)
	}
	return &cp
}

func redactedExternalScanCopy(m *ExternalScan) *ExternalScan {
	if m == nil {
		return nil
	}
	cp := *m
	if hasIcebergRuntime(m) {
		cp.FileList = redactStringPaths(m.FileList)
		cp.IcebergObjectIoRef = redactNonEmpty(cp.IcebergObjectIoRef)
		cp.IcebergDataTasks = redactDataTasks(m.IcebergDataTasks)
		cp.IcebergDeleteTasks = redactDeleteTasks(m.IcebergDeleteTasks)
	}
	return &cp
}

func hasIcebergRuntime(m *ExternalScan) bool {
	return m != nil && (m.IcebergObjectIoRef != "" ||
		len(m.IcebergDataTasks) > 0 ||
		len(m.IcebergDeleteTasks) > 0 ||
		m.IcebergSnapshot != nil)
}

func redactDataTasks(in []*IcebergDataFileTask) []*IcebergDataFileTask {
	if len(in) == 0 {
		return nil
	}
	out := make([]*IcebergDataFileTask, 0, len(in))
	for _, task := range in {
		if task == nil {
			out = append(out, nil)
			continue
		}
		cp := *task
		cp.FilePath = api.RedactPath(cp.FilePath)
		cp.CredentialScope = redactNonEmpty(cp.CredentialScope)
		out = append(out, &cp)
	}
	return out
}

func redactDeleteTasks(in []*IcebergDeleteFileTask) []*IcebergDeleteFileTask {
	if len(in) == 0 {
		return nil
	}
	out := make([]*IcebergDeleteFileTask, 0, len(in))
	for _, task := range in {
		if task == nil {
			out = append(out, nil)
			continue
		}
		cp := *task
		cp.DeleteFilePath = api.RedactPath(cp.DeleteFilePath)
		cp.ReferencedDataFile = api.RedactPath(cp.ReferencedDataFile)
		cp.CredentialScope = redactNonEmpty(cp.CredentialScope)
		out = append(out, &cp)
	}
	return out
}

func redactStringPaths(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	out := make([]string, len(in))
	for i, value := range in {
		out[i] = api.RedactPath(value)
	}
	return out
}

func redactNonEmpty(value string) string {
	if value == "" {
		return ""
	}
	return api.RedactedValue
}

func redactedBytes(n int) string {
	if n <= 0 {
		return ""
	}
	return fmt.Sprintf("<redacted:%d bytes>", n)
}
