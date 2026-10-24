// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package plan

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type temporaryAccessTestSession struct {
	physical string
}

func (s *temporaryAccessTestSession) GetTempTable(string, string) (string, bool) { return "", false }
func (s *temporaryAccessTestSession) AddTempTable(string, string, string)        {}
func (s *temporaryAccessTestSession) RemoveTempTable(string, string)             {}
func (s *temporaryAccessTestSession) RemoveTempTableByRealName(string)           {}
func (s *temporaryAccessTestSession) GetSqlModeNoAutoValueOnZero() (bool, bool)  { return false, false }
func (s *temporaryAccessTestSession) OwnsTemporaryTable(_, physical string) bool {
	return s.physical == physical
}
func (s *temporaryAccessTestSession) CheckTemporaryTableCapacity(context.Context) error {
	return nil
}
func (s *temporaryAccessTestSession) PublishTemporaryTable(string, string, string)          {}
func (s *temporaryAccessTestSession) RetireTemporaryTable(string, string, string, []string) {}

var _ process.TemporaryTableDDL = (*temporaryAccessTestSession)(nil)

func TestTemporaryTableAccessAllowed(t *testing.T) {
	physical := defines.GenTempTableName(uuid.MustParse("00112233-4455-6677-8899-aabbccddeeff"), "db", "t")
	session := &temporaryAccessTestSession{physical: physical}
	tests := []struct {
		name          string
		ctx           context.Context
		tableDef      *TableDef
		physical      string
		aliasResolved bool
		want          bool
	}{
		{name: "owner root", tableDef: &TableDef{TableType: catalog.SystemTemporaryTable}, physical: physical, want: true},
		{name: "foreign root", tableDef: &TableDef{TableType: catalog.SystemTemporaryTable}, physical: physical + "_other", want: false},
		{name: "ordinary prefix remains visible", tableDef: &TableDef{TableType: catalog.SystemOrdinaryRel}, physical: physical + "_ordinary", want: true},
		{name: "staging alias", tableDef: &TableDef{TableType: catalog.SystemTemporaryTable}, physical: physical + "_staged", aliasResolved: true, want: true},
		{name: "trusted cleanup", ctx: defines.AttachTemporaryTableAccess(context.Background()), tableDef: &TableDef{TableType: catalog.SystemTemporaryTable}, physical: physical + "_other", want: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, TemporaryTableAccessAllowed(test.ctx, session, "db", test.physical, test.tableDef, test.aliasResolved))
		})
	}
}
