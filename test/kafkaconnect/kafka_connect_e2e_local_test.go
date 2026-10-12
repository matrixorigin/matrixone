// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package main

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"
)

type staticDrainStateReader struct {
	rowsValue        []row
	restOffsetValue  int64
	groupOffsetValue int64
	groupFound       bool
	logEndValue      int64
	logEndFound      bool
}

func (r staticDrainStateReader) rows(context.Context) ([]row, error) {
	return r.rowsValue, nil
}

func (r staticDrainStateReader) restOffset(context.Context) (int64, error) {
	return r.restOffsetValue, nil
}

func (r staticDrainStateReader) groupOffset(context.Context) (int64, bool, error) {
	return r.groupOffsetValue, r.groupFound, nil
}

func (r staticDrainStateReader) logEndOffset(context.Context) (int64, bool, error) {
	return r.logEndValue, r.logEndFound, nil
}

func TestWaitForDrainReturnsEveryVerifiedOffset(t *testing.T) {
	rows := expectedFinalRows()
	reader := staticDrainStateReader{
		rowsValue:        sortedRows(rows),
		restOffsetValue:  7,
		groupOffsetValue: 7,
		groupFound:       true,
		logEndValue:      7,
		logEndFound:      true,
	}
	receipt, err := waitForDrain(t.Context(), reader, rows, 7)
	if err != nil {
		t.Fatal(err)
	}
	if want := (drainReceipt{RESTOffset: 7, KafkaGroupOffset: 7, KafkaLogEndOffset: 7}); !reflect.DeepEqual(receipt, want) {
		t.Fatalf("drain receipt = %+v, want %+v", receipt, want)
	}
}

func TestWaitForDrainRejectsGroupOffsetBehindSinkAndLogEnd(t *testing.T) {
	reader := staticDrainStateReader{
		rowsValue:        sortedRows(expectedFinalRows()),
		restOffsetValue:  7,
		groupOffsetValue: 6,
		groupFound:       true,
		logEndValue:      7,
		logEndFound:      true,
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Millisecond)
	defer cancel()
	if _, err := waitForDrain(ctx, reader, expectedFinalRows(), 7); err == nil {
		t.Fatal("waitForDrain accepted a lagging Kafka consumer group")
	}
}

func TestPhaseReportIncludesZeroOffsets(t *testing.T) {
	data, err := json.Marshal(phase{})
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"expected_offset", "rest_offset", "kafka_group_offset", "kafka_log_end_offset"} {
		if _, exists := fields[name]; !exists {
			t.Errorf("phase receipt omitted %s at offset zero: %s", name, data)
		}
	}
}

func TestRunResultMarksFailedReport(t *testing.T) {
	report := runReport{Status: "running"}
	setRunResult(&report, errors.New("sink offset did not reach the expected value"))
	data, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		Status string `json:"status"`
		Error  string `json:"error"`
	}
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if got.Status != "failed" || got.Error != "sink offset did not reach the expected value" {
		t.Fatalf("failed run report = %+v, want failed status and error", got)
	}
}

func TestExpectedFinalRowsHaveUniqueIDsAndCoverRecoveryContract(t *testing.T) {
	rows := expectedFinalRows()
	if err := validateUniqueRowIDs(rows); err != nil {
		t.Fatalf("final recovery fixture must contain each primary key once: %v", err)
	}
	if len(rows) != 4 {
		t.Fatalf("final row count = %d, want 4", len(rows))
	}
	byID := make(map[int64]row, len(rows))
	for _, item := range rows {
		byID[item.ID] = item
	}
	if _, exists := byID[1]; exists {
		t.Fatal("final recovery fixture must omit the tombstoned row with ID 1")
	}
	if got := byID[2].Note; got == nil || *got != "" {
		t.Fatalf("ID 2 note = %v, want a non-NULL empty string", got)
	}
	if got := byID[3]; got.Name != "after-update" || got.Counter != 33 || got.Note == nil || *got.Note != "更新" {
		t.Fatalf("ID 3 = %+v, want the updated row", got)
	}
	if got := byID[5].Note; got != nil {
		t.Fatalf("ID 5 note = %q, want SQL NULL", *got)
	}

	duplicate := append(append([]row(nil), rows...), rows[0])
	if err := validateUniqueRowIDs(duplicate); err == nil || !errors.Is(err, errDuplicateRowID) {
		t.Fatalf("duplicate primary key error = %v, want duplicate-row error", err)
	}
}

func TestJSONConverterEnvelopesPreserveKeyAndNullSemantics(t *testing.T) {
	key, err := encodeKey(17)
	if err != nil {
		t.Fatal(err)
	}
	var decodedKey struct {
		Schema struct {
			Fields []struct {
				Field string `json:"field"`
			} `json:"fields"`
		} `json:"schema"`
		Payload map[string]int64 `json:"payload"`
	}
	if err := json.Unmarshal(key, &decodedKey); err != nil {
		t.Fatal(err)
	}
	if len(decodedKey.Schema.Fields) != 1 || decodedKey.Schema.Fields[0].Field != "id" || decodedKey.Payload["id"] != 17 {
		t.Fatalf("unexpected Kafka key envelope: %s", key)
	}

	for _, test := range []struct {
		name string
		note *string
		want any
	}{
		{name: "SQL NULL", note: nil, want: nil},
		{name: "empty string", note: stringPointer(""), want: ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			value, err := encodeValue(row{ID: 17, Name: "fixture", Note: test.note, Counter: 1})
			if err != nil {
				t.Fatal(err)
			}
			var decoded struct {
				Schema struct {
					Fields []struct {
						Field    string `json:"field"`
						Optional bool   `json:"optional"`
					} `json:"fields"`
				} `json:"schema"`
				Payload map[string]any `json:"payload"`
			}
			if err := json.Unmarshal(value, &decoded); err != nil {
				t.Fatal(err)
			}
			if len(decoded.Schema.Fields) != 4 || decoded.Schema.Fields[2].Field != "note" || !decoded.Schema.Fields[2].Optional {
				t.Fatalf("note must be an optional schema field: %s", value)
			}
			if decoded.Payload["note"] != test.want {
				t.Fatalf("note payload = %#v, want %#v", decoded.Payload["note"], test.want)
			}
		})
	}
}
