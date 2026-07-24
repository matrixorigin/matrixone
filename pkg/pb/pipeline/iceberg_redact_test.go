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
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/gogo/protobuf/gogoproto"
	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/protoc-gen-gogo/descriptor"
	"github.com/matrixorigin/matrixone/pkg/iceberg/testutil"
)

func TestIcebergRedactionDisablesGeneratedStringers(t *testing.T) {
	file := decodePipelineFileDescriptor(t)
	for _, name := range []string{
		"Message",
		"IcebergDataFileTask",
		"IcebergDeleteFileTask",
		"ExternalScan",
		"Instruction",
		"Pipeline",
	} {
		var found *descriptor.DescriptorProto
		for _, message := range file.GetMessageType() {
			if message.GetName() == name {
				found = message
				break
			}
		}
		if found == nil {
			t.Fatalf("pipeline descriptor missing protected message %s", name)
		}
		if gogoproto.EnabledGoStringer(file, found) {
			t.Fatalf("generated String method must stay disabled for %s", name)
		}
	}
}

func decodePipelineFileDescriptor(t *testing.T) *descriptor.FileDescriptorProto {
	t.Helper()
	compressed := proto.FileDescriptor("pipeline.proto")
	if len(compressed) == 0 {
		t.Fatal("pipeline protobuf descriptor is not registered")
	}
	reader, err := gzip.NewReader(bytes.NewReader(compressed))
	if err != nil {
		t.Fatalf("open pipeline protobuf descriptor: %v", err)
	}
	defer reader.Close()
	raw, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read pipeline protobuf descriptor: %v", err)
	}
	var file descriptor.FileDescriptorProto
	if err := proto.Unmarshal(raw, &file); err != nil {
		t.Fatalf("decode pipeline protobuf descriptor: %v", err)
	}
	return &file
}

func TestIcebergRuntimeStringRedactsSensitiveFields(t *testing.T) {
	rawCredential := "scope://catalog/secret-token"
	rawObjectRef := "object-scope-ref-with-secret"
	rawDataPath := "s3://warehouse/sales/orders/data-0001.parquet"
	rawDeletePath := "s3://warehouse/sales/orders/delete-0001.parquet"
	rawReferencedPath := "s3://warehouse/sales/orders/data-0001.parquet?X-Amz-Signature=secret"

	dataTask := &IcebergDataFileTask{
		FilePath:        rawDataPath,
		FileFormat:      "parquet",
		CredentialScope: rawCredential,
	}
	deleteTask := &IcebergDeleteFileTask{
		DeleteFilePath:     rawDeletePath,
		ReferencedDataFile: rawReferencedPath,
		CredentialScope:    rawCredential,
	}
	scan := &ExternalScan{
		FileList:           []string{rawDataPath},
		IcebergDataTasks:   []*IcebergDataFileTask{dataTask},
		IcebergDeleteTasks: []*IcebergDeleteFileTask{deleteTask},
		IcebergObjectIoRef: rawObjectRef,
	}

	assertRedactedString(t, dataTask.String(), rawCredential, rawDataPath, "warehouse", "orders")
	assertRedactedString(t, deleteTask.String(), rawCredential, rawDeletePath, rawReferencedPath, "X-Amz-Signature")
	assertRedactedString(t, scan.String(), rawCredential, rawObjectRef, rawDataPath, rawDeletePath, "X-Amz-Signature")
	if !strings.Contains(scan.String(), "<redacted>") || !strings.Contains(scan.String(), "<redacted:path:") {
		t.Fatalf("expected redaction markers in scan string: %s", scan.String())
	}
}

func TestIcebergRuntimeInstructionAndPipelineStringRedactNestedExternalScan(t *testing.T) {
	rawCredential := "scope://catalog/secret-token"
	rawObjectRef := "object-scope-ref-with-secret"
	rawDataPath := "s3://warehouse/sales/orders/data-0001.parquet"
	rawDeletePath := "s3://warehouse/sales/orders/delete-0001.parquet"

	instruction := &Instruction{
		ExternalScan: &ExternalScan{
			FileList:           []string{rawDataPath},
			IcebergObjectIoRef: rawObjectRef,
			IcebergDataTasks: []*IcebergDataFileTask{{
				FilePath:        rawDataPath,
				CredentialScope: rawCredential,
			}},
			IcebergDeleteTasks: []*IcebergDeleteFileTask{{
				DeleteFilePath:  rawDeletePath,
				CredentialScope: rawCredential,
			}},
		},
	}
	pipeline := &Pipeline{
		InstructionList: []*Instruction{instruction},
		Children: []*Pipeline{{
			InstructionList: []*Instruction{instruction},
		}},
	}

	assertRedactedString(t, instruction.String(), rawCredential, rawObjectRef, rawDataPath, rawDeletePath, "warehouse", "orders")
	assertRedactedString(t, pipeline.String(), rawCredential, rawObjectRef, rawDataPath, rawDeletePath, "warehouse", "orders")
	if instruction.ExternalScan.IcebergObjectIoRef != rawObjectRef || instruction.ExternalScan.IcebergDataTasks[0].CredentialScope != rawCredential {
		t.Fatalf("string redaction must not mutate runtime payload: %+v", instruction.ExternalScan)
	}
}

func TestIcebergRuntimeFmtDebugOutputRedactsSensitiveFields(t *testing.T) {
	rawCredential := "scope://catalog/secret-token"
	rawObjectRef := "object-scope-ref-with-secret"
	rawDataPath := "s3://warehouse/sales/orders/data-0001.parquet"
	rawDeletePath := "s3://warehouse/sales/orders/delete-0001.parquet"
	instruction := &Instruction{
		ExternalScan: &ExternalScan{
			FileList:           []string{rawDataPath},
			IcebergObjectIoRef: rawObjectRef,
			IcebergDataTasks: []*IcebergDataFileTask{{
				FilePath:        rawDataPath,
				CredentialScope: rawCredential,
			}},
			IcebergDeleteTasks: []*IcebergDeleteFileTask{{
				DeleteFilePath:  rawDeletePath,
				CredentialScope: rawCredential,
			}},
		},
	}
	pipeline := &Pipeline{InstructionList: []*Instruction{instruction}}
	for name, got := range map[string]string{
		"data_task_v":     fmt.Sprintf("%+v", instruction.ExternalScan.IcebergDataTasks[0]),
		"data_task_go":    fmt.Sprintf("%#v", instruction.ExternalScan.IcebergDataTasks[0]),
		"delete_task_v":   fmt.Sprintf("%+v", instruction.ExternalScan.IcebergDeleteTasks[0]),
		"delete_task_go":  fmt.Sprintf("%#v", instruction.ExternalScan.IcebergDeleteTasks[0]),
		"external_scan_v": fmt.Sprintf("%+v", instruction.ExternalScan),
		"external_scan_go": fmt.Sprintf(
			"%#v",
			instruction.ExternalScan,
		),
		"instruction_v":  fmt.Sprintf("%+v", instruction),
		"instruction_go": fmt.Sprintf("%#v", instruction),
		"pipeline_v":     fmt.Sprintf("%+v", pipeline),
		"pipeline_go":    fmt.Sprintf("%#v", pipeline),
	} {
		assertRedactedString(t, got, rawCredential, rawObjectRef, rawDataPath, rawDeletePath, "warehouse", "orders")
		if !strings.Contains(got, "<redacted>") {
			t.Fatalf("%s expected redaction marker, got %s", name, got)
		}
	}
}

func TestIcebergRuntimeJSONOutputRedactsSensitiveFields(t *testing.T) {
	rawCredential := "scope://catalog/secret-token"
	rawObjectRef := "object-scope-ref-with-secret"
	rawDataPath := "s3://warehouse/sales/orders/data-0001.parquet"
	rawDeletePath := "s3://warehouse/sales/orders/delete-0001.parquet"
	instruction := &Instruction{
		ExternalScan: &ExternalScan{
			FileList:           []string{rawDataPath},
			IcebergObjectIoRef: rawObjectRef,
			IcebergDataTasks: []*IcebergDataFileTask{{
				FilePath:        rawDataPath,
				CredentialScope: rawCredential,
			}},
			IcebergDeleteTasks: []*IcebergDeleteFileTask{{
				DeleteFilePath:     rawDeletePath,
				ReferencedDataFile: rawDataPath + "?X-Amz-Signature=secret",
				CredentialScope:    rawCredential,
			}},
		},
	}
	pipeline := &Pipeline{InstructionList: []*Instruction{instruction}}
	for name, value := range map[string]any{
		"data_task_json":    instruction.ExternalScan.IcebergDataTasks[0],
		"delete_task_json":  instruction.ExternalScan.IcebergDeleteTasks[0],
		"external_json":     instruction.ExternalScan,
		"instruction_json":  instruction,
		"pipeline_json":     pipeline,
		"pipeline_json_ptr": &pipeline,
	} {
		data, err := json.Marshal(value)
		if err != nil {
			t.Fatalf("%s marshal json: %v", name, err)
		}
		got := string(data)
		assertRedactedString(t, got, rawCredential, rawObjectRef, rawDataPath, rawDeletePath, "warehouse", "orders", "X-Amz-Signature")
		if !containsRedactionMarker(got) {
			t.Fatalf("%s expected redaction marker, got %s", name, got)
		}
	}
	if instruction.ExternalScan.IcebergObjectIoRef != rawObjectRef || instruction.ExternalScan.IcebergDataTasks[0].CredentialScope != rawCredential {
		t.Fatalf("json redaction must not mutate runtime payload: %+v", instruction.ExternalScan)
	}
}

func TestPipelineMessageStringDoesNotDumpEncodedIcebergRuntime(t *testing.T) {
	rawCredential := "scope://catalog/secret-token"
	rawObjectRef := "object-scope-ref-with-secret"
	rawDataPath := "s3://warehouse/sales/orders/data-0001.parquet"
	pipelinePayload := &Pipeline{
		InstructionList: []*Instruction{{
			ExternalScan: &ExternalScan{
				FileList:           []string{rawDataPath},
				IcebergObjectIoRef: rawObjectRef,
				IcebergDataTasks: []*IcebergDataFileTask{{
					FilePath:        rawDataPath,
					CredentialScope: rawCredential,
				}},
			},
		}},
	}
	data, err := pipelinePayload.Marshal()
	if err != nil {
		t.Fatalf("marshal pipeline payload: %v", err)
	}
	msg := &Message{
		Sid:          Status_Last,
		Cmd:          Method_PipelineMessage,
		Data:         data,
		ProcInfoData: []byte(rawCredential),
	}

	for name, got := range map[string]string{
		"message_v":      fmt.Sprintf("%v", msg),
		"message_plus_v": fmt.Sprintf("%+v", msg),
		"message_go":     fmt.Sprintf("%#v", msg),
		"debug_string":   msg.DebugString(),
	} {
		assertRedactedString(t, got, rawCredential, rawObjectRef, rawDataPath, "warehouse", "orders")
		if strings.Contains(name, "message") && !strings.Contains(got, "data:<redacted:") {
			t.Fatalf("%s expected redacted data marker, got %s", name, got)
		}
	}
	if len(msg.Data) != len(data) || string(msg.ProcInfoData) != rawCredential {
		t.Fatalf("message string redaction must not mutate wire payload")
	}
}

func TestPipelineMessageJSONDoesNotDumpEncodedIcebergRuntime(t *testing.T) {
	rawCredential := "scope://catalog/secret-token"
	rawObjectRef := "object-scope-ref-with-secret"
	rawDataPath := "s3://warehouse/sales/orders/data-0001.parquet"
	pipelinePayload := &Pipeline{
		InstructionList: []*Instruction{{
			ExternalScan: &ExternalScan{
				FileList:           []string{rawDataPath},
				IcebergObjectIoRef: rawObjectRef,
				IcebergDataTasks: []*IcebergDataFileTask{{
					FilePath:        rawDataPath,
					CredentialScope: rawCredential,
				}},
			},
		}},
	}
	data, err := pipelinePayload.Marshal()
	if err != nil {
		t.Fatalf("marshal pipeline payload: %v", err)
	}
	msg := &Message{
		Sid:          Status_Last,
		Cmd:          Method_PipelineMessage,
		Data:         data,
		ProcInfoData: []byte(rawCredential),
		DebugMsg:     rawCredential,
	}

	raw, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal message json: %v", err)
	}
	got := string(raw)
	assertRedactedString(t, got, rawCredential, rawObjectRef, rawDataPath, "warehouse", "orders")
	if !strings.Contains(got, "data") || !containsRedactionMarker(got) {
		t.Fatalf("expected redacted data marker, got %s", got)
	}
	if len(msg.Data) != len(data) || string(msg.ProcInfoData) != rawCredential || msg.DebugMsg != rawCredential {
		t.Fatalf("message json redaction must not mutate wire payload")
	}
}

func TestExternalScanStringLeavesLegacyFileListVisible(t *testing.T) {
	scan := &ExternalScan{FileList: []string{"etl-file.csv"}}
	got := scan.String()
	if !strings.Contains(got, "etl-file.csv") {
		t.Fatalf("legacy non-Iceberg external scan string should remain unchanged, got %s", got)
	}
}

func TestIcebergDataFileTaskResidualFilterRoundTrip(t *testing.T) {
	task := &IcebergDataFileTask{
		FilePath:           "s3://warehouse/sales/orders/data.parquet",
		HasResidualFilter:  true,
		ResidualFilterHash: "aabbcc",
	}
	data, err := proto.Marshal(task)
	if err != nil {
		t.Fatalf("marshal task: %v", err)
	}
	var decoded IcebergDataFileTask
	if err := proto.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal task: %v", err)
	}
	if !decoded.HasResidualFilter || decoded.ResidualFilterHash != "aabbcc" {
		t.Fatalf("residual filter fields did not round trip: %+v", decoded)
	}
}

func assertRedactedString(t *testing.T, got string, forbidden ...string) {
	t.Helper()
	testutil.AssertNoIcebergSensitiveLeak(t, "Iceberg pipeline debug output", got, forbidden...)
}

func containsRedactionMarker(got string) bool {
	return testutil.HasIcebergRedactionMarker(got)
}
