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

package plan

import (
	"context"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/iceberg/maintenance"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func TestIcebergBuiltinCallIsInterceptedBeforeStoredProcedure(t *testing.T) {
	stmt, err := mysql.ParseOne(context.Background(), "call iceberg_rewrite_manifests('ksa_gold.sales.orders', 'ref=main')", 1)
	if err != nil {
		t.Fatalf("parse Iceberg CALL: %v", err)
	}
	call, ok := stmt.(*tree.CallStmt)
	if !ok {
		t.Fatalf("expected CallStmt, got %T", stmt)
	}
	if !isIcebergBuiltinCall(call) {
		t.Fatalf("expected Iceberg builtin call to be recognized")
	}
	_, err = BuildPlan(NewMockCompilerContext(true), stmt, false)
	if err == nil || !strings.Contains(err.Error(), "Iceberg builtin procedure iceberg_rewrite_manifests for ksa_gold.sales.orders is recognized") {
		t.Fatalf("expected Iceberg builtin procedure not-supported error, got %v", err)
	}
	if strings.Contains(err.Error(), "mo_stored_procedure") || strings.Contains(err.Error(), "statement:") {
		t.Fatalf("Iceberg CALL should not fall through to generic CALL/stored procedure path: %v", err)
	}
}

func TestIcebergBuiltinCallParsesProcedureArguments(t *testing.T) {
	stmt, err := mysql.ParseOne(context.Background(), "call iceberg_rewrite_data_files('ksa_gold.sales.orders', 'ref=main,target_file_size=268435456')", 1)
	if err != nil {
		t.Fatalf("parse Iceberg CALL: %v", err)
	}
	call, ok := stmt.(*tree.CallStmt)
	if !ok {
		t.Fatalf("expected CallStmt, got %T", stmt)
	}
	bound, err := parseIcebergBuiltinCall(context.Background(), call)
	if err != nil {
		t.Fatalf("bind Iceberg CALL: %v", err)
	}
	if bound.Parsed.Operation != maintenance.OperationRewriteDataFiles || bound.Parsed.Target != "ksa_gold.sales.orders" || bound.Parsed.Options["ref"] != "main" || bound.Parsed.Options["target_file_size"] != "268435456" {
		t.Fatalf("unexpected bound Iceberg CALL: %+v", bound)
	}
	if bound.Parsed.TargetID.Catalog != "ksa_gold" || bound.Parsed.TargetID.Namespace != "sales" || bound.Parsed.TargetID.Table != "orders" {
		t.Fatalf("unexpected bound Iceberg CALL target: %+v", bound.Parsed.TargetID)
	}
}

func TestIcebergBuiltinCallRejectsNonStringArguments(t *testing.T) {
	stmt, err := mysql.ParseOne(context.Background(), "call iceberg_rewrite_manifests(42)", 1)
	if err != nil {
		t.Fatalf("parse Iceberg CALL: %v", err)
	}
	call := stmt.(*tree.CallStmt)
	_, err = parseIcebergBuiltinCall(context.Background(), call)
	if err == nil || !strings.Contains(err.Error(), "requires target as a string literal") {
		t.Fatalf("expected string literal validation error, got %v", err)
	}
}

func TestQualifiedIcebergCallIsNotBuiltin(t *testing.T) {
	stmt, err := mysql.ParseOne(context.Background(), "call app.iceberg_rewrite_manifests()", 1)
	if err != nil {
		t.Fatalf("parse qualified CALL: %v", err)
	}
	call, ok := stmt.(*tree.CallStmt)
	if !ok {
		t.Fatalf("expected CallStmt, got %T", stmt)
	}
	if isIcebergBuiltinCall(call) {
		t.Fatalf("qualified procedure names should not be treated as Iceberg builtins")
	}
}
