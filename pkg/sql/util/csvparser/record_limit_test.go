// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
package csvparser

import (
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestRecordLimitCoversShortFieldsAndQuotes(t *testing.T) {
	for _, input := range []string{strings.Repeat("x,", 40) + "x\n", "\"" + strings.Repeat("x,", 40) + "x\"\n", strings.Repeat(",", 81) + "\n"} {
		p, err := NewCSVParser(&CSVConfig{FieldsTerminatedBy: ",", FieldsEnclosedBy: "\"", LinesTerminatedBy: "\n"}, strings.NewReader(input), 128, false)
		require.NoError(t, err)
		p.recordLimit = 32
		_, err = p.Read(nil)
		require.ErrorContains(t, err, "txn-entry-size-limit")
		require.LessOrEqual(t, len(p.recordBuffer), 32, "check before growing the accumulated record")
	}
}

func TestRecordLimitResetsForNextLine(t *testing.T) {
	p, err := NewCSVParser(&CSVConfig{FieldsTerminatedBy: ",", LinesTerminatedBy: "\n"}, strings.NewReader(strings.Repeat("abc,def\n", 10)), 4, false)
	require.NoError(t, err)
	p.recordLimit = 8
	for range 10 {
		row, readErr := p.Read(nil)
		require.NoError(t, readErr)
		require.Equal(t, "abc", row[0].Val)
		require.Equal(t, "def", row[1].Val)
	}
}
