// Copyright 2021-2026 Matrix Origin
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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	plan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	dmysql "github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

// TestFormatInfileExternalOptionsPreservesFilePathForPlainInfile covers issue
// #31261: an ordinary read-only local infile external table must keep its
// FILEPATH in SHOW CREATE TABLE output, otherwise the replayed DDL builds a
// table that cannot read the source file.
func TestFormatInfileExternalOptionsPreservesFilePathForPlainInfile(t *testing.T) {
	param := &tree.ExternParam{ExParamConst: tree.ExParamConst{
		Format:   tree.CSV,
		Filepath: "/data/ext rows/rows.csv",
	}}
	out := formatInfileExternalOptionsForShowCreate(param, "")
	require.Contains(t, out, "'FILEPATH'='/data/ext rows/rows.csv'")
	require.Contains(t, out, "'FORMAT'='csv'")
	// Empty optional keys must be omitted: the read-side option validator
	// rejects e.g. 'JSONDATA'='', so emitting them breaks replayed tables.
	require.NotContains(t, out, "JSONDATA")
	require.NotContains(t, out, "COMPRESSION")
}

// TestFormatInfileExternalOptionsEscapesInfilePath: the emitted FILEPATH is a
// SQL literal that reparses to the original path in the default SQL mode and
// under NO_BACKSLASH_ESCAPES.
func TestFormatInfileExternalOptionsEscapesInfilePath(t *testing.T) {
	param := &tree.ExternParam{ExParamConst: tree.ExParamConst{
		Format:   tree.CSV,
		Filepath: `/data/o'brien\dir rows.csv`,
	}}
	out := formatInfileExternalOptionsForShowCreate(param, "")
	require.Contains(t, out, `'FILEPATH'='/data/o''brien\\dir rows.csv'`)

	outNoEsc := formatInfileExternalOptionsForShowCreate(param, "NO_BACKSLASH_ESCAPES")
	require.Contains(t, outNoEsc, `'FILEPATH'='/data/o''brien\dir rows.csv'`)
}

// TestFormatInfileExternalOptionsKeepsHiveAndNonEmptyOptionals keeps the
// #24372 shape: Hive-partitioned external tables retain FILEPATH plus the
// HIVE_PARTITION* keys, and non-empty optional keys still round-trip.
func TestFormatInfileExternalOptionsKeepsHiveAndNonEmptyOptionals(t *testing.T) {
	param := &tree.ExternParam{ExParamConst: tree.ExParamConst{
		Format:            tree.CSV,
		Filepath:          "/data/test/",
		CompressType:      "auto",
		HivePartitioning:  true,
		HivePartitionCols: []string{"part_id"},
	}}
	out := formatInfileExternalOptionsForShowCreate(param, "")
	require.Contains(t, out, "'FILEPATH'='/data/test/'")
	require.Contains(t, out, "'COMPRESSION'='auto'")
	require.Contains(t, out, "'HIVE_PARTITIONING'='true'")
	require.Contains(t, out, "'HIVE_PARTITION_COLUMNS'='part_id'")

	// A JSON-format infile table must retain its non-empty JSONDATA option.
	jsonParam := &tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			Format:   "jsonline",
			Filepath: "/data/x.jsonl",
		},
		ExParam: tree.ExParam{JsonData: "object"},
	}
	out = formatInfileExternalOptionsForShowCreate(jsonParam, "")
	require.Contains(t, out, "'FILEPATH'='/data/x.jsonl'")
	require.Contains(t, out, "'FORMAT'='jsonline'")
	require.Contains(t, out, "'JSONDATA'='object'")
}

// TestFormatInfileExternalOptionsMasksServiceArgumentCredentials covers the
// credential-disclosure case of #31261: an INFILE FILEPATH may embed
// object-storage credentials in service-argument syntax
// (fileservice path.go / get.go grammar), and SHOW CREATE output — which
// mo-dump captures verbatim — must redact them while retaining ordinary
// local paths and non-credential arguments byte-for-byte.
func TestFormatInfileExternalOptionsMasksServiceArgumentCredentials(t *testing.T) {
	type fn struct{ name, in, want string }
	cases := []fn{
		{
			name: "s3-opts key-value credentials masked, location retained",
			in:   "s3-opts,endpoint=https://owned.invalid,region=us-east-1,bucket=b,key=AKIAPROBE00001,secret=SECRET-PROBE-AAAA:rows.csv",
			want: "s3-opts,endpoint=https://owned.invalid,region=us-east-1,bucket=b,key=<redacted>,secret=<redacted>:rows.csv",
		},
		{
			name: "opts alias with upper-case keys masked",
			in:   "OPTS,ENDPOINT=e,KEY=KEY-PROBE-BBBB,SECRET=SECRET-PROBE-BBBB:dir/a.csv",
			want: "OPTS,ENDPOINT=e,KEY=<redacted>,SECRET=<redacted>:dir/a.csv",
		},
		{
			name: "positional s3 credentials masked, prefix retained",
			in:   "s3,ep,region,bucket,KEY-PROBE-CCCC,SECRET-PROBE-CCCC,pfx:rows.csv",
			want: "s3,ep,region,bucket,<redacted>,<redacted>,pfx:rows.csv",
		},
		{
			name: "positional minio credentials masked",
			in:   "minio,ep,region,bucket,KEY-PROBE-DDDD,SECRET-PROBE-DDDD,pfx:rows.csv",
			want: "minio,ep,region,bucket,<redacted>,<redacted>,pfx:rows.csv",
		},
		{
			name: "hdfs kerberos password masked, user retained",
			in:   "hdfs,user=svc,kerberos-password=KPASS-PROBE-EEEE:/in.csv",
			want: "hdfs,user=svc,kerberos-password=<redacted>:/in.csv",
		},
		{
			name: "token aliases masked",
			in:   "s3-opts,bearer-token=BT-FFFF,security-token=ST-FFFF,session-token=TK-FFFF,token=TK2-FFFF,endpoint=e:rows.csv",
			want: "s3-opts,bearer-token=<redacted>,security-token=<redacted>,session-token=<redacted>,token=<redacted>,endpoint=e:rows.csv",
		},
		{
			name: "quoted endpoint with embedded comma preserved, credentials masked",
			in:   `s3-opts,"endpoint=https://a,b:8443",key=KEY-PROBE-QQQQ,secret=SECRET-PROBE-QQQQ:rows.csv`,
			want: `s3-opts,"endpoint=https://a,b:8443",key=<redacted>,secret=<redacted>:rows.csv`,
		},
		{
			name: "value containing colon kept after last-colon split",
			in:   "s3-opts,endpoint=https://e:8443,key=KEY-PROBE-RRRR:rows.csv",
			want: "s3-opts,endpoint=https://e:8443,key=<redacted>:rows.csv",
		},
		{
			name: "fail closed on unclosed quote in service-claimed section",
			in:   `"s3-opts,unclosed:rows.csv`,
			want: "s3-opts,<redacted>:rows.csv",
		},
		{
			name: "short positional s3 key zone is fail-closed",
			in:   "s3,ep,region,bucket,KEYPOS5A:rows.csv",
			want: "s3,ep,region,bucket,<redacted>:rows.csv",
		},
		{
			name: "colonless credential-claimed s3-opts is fail-closed",
			in:   "s3-opts,key=KEYNC1S3,secret=SECNC1S3",
			want: "s3-opts,<redacted>",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := maskServiceArgumentCredentialsForShowCreate(c.in)
			if got != c.want {
				t.Fatalf("masked mismatch:\n  in:   %s\n  want: %s\n  got:  %s", c.in, c.want, got)
			}
		})
	}

	unchanged := []string{
		// ordinary local paths keep exact bytes (#31261 core behavior)
		"/data/ext rows/rows.csv",
		"/data/o'brien\\dir rows.csv",
		// colon in a plain local filename is not a service section
		"./dir/name:part.csv",
		// positional credential-free service: nothing to redact
		"s3-no-key,ep,region,bucket,pfx:rows.csv",
		// unknown services pass through untouched
		"etl,foo=bar:x.csv",
		// colonless strings without a service claim are plain local paths
		"data,row.csv",
		// leading-colon path has no service section
		":dir/file.csv",
		// minimal positional s3 without a credential pair stays untouched
		// (below the production grammar's credential-bearing arity)
		"s3,ep:rows.csv",
	}
	for _, in := range unchanged {
		if got := maskServiceArgumentCredentialsForShowCreate(in); got != in {
			t.Fatalf("unchanged case rewritten:\n  in:  %s\n  got: %s", in, got)
		}
	}

	// identity handles stay visible, matching the explicit-S3 renderer
	// which shows role_arn/external_id
	roleCase := "s3-opts,role-arn=arn:active:dd002491:role/reader,external-id=eid001,key=KEY-PROBE-GGGG:rows.csv"
	if got, want := maskServiceArgumentCredentialsForShowCreate(roleCase),
		"s3-opts,role-arn=arn:active:dd002491:role/reader,external-id=eid001,key=<redacted>:rows.csv"; got != want {
		t.Fatalf("identity handles rewritten:\n  want: %s\n  got:  %s", want, got)
	}

	// Full SHOW CREATE composition: exactly one masked FILEPATH clause, and
	// none of the synthetic credential sentinels survive into the output.
	param := &tree.ExternParam{ExParamConst: tree.ExParamConst{
		Format:   tree.CSV,
		Filepath: "s3-opts,endpoint=https://owned.invalid,region=us-east-1,bucket=b,key=AKIA-PROBE-1111,secret=SECRET-PROBE-2222:rows.csv",
	}}
	out := formatInfileExternalOptionsForShowCreate(param, "")
	wantClause := " INFILE{'FILEPATH'='s3-opts,endpoint=https://owned.invalid,region=us-east-1,bucket=b,key=<redacted>,secret=<redacted>:rows.csv','FORMAT'='csv'}"
	require.Equal(t, wantClause, out)
	for _, sentinel := range []string{"AKIA-PROBE-1111", "SECRET-PROBE-2222", "AKIAPROBE00001"} {
		require.NotContains(t, out, sentinel)
	}
	// role-arn and external-id are not credentials under this renderer's rule
	roleParam := &tree.ExternParam{ExParamConst: tree.ExParamConst{
		Format:   tree.CSV,
		Filepath: roleCase,
	}}
	roleOut := formatInfileExternalOptionsForShowCreate(roleParam, "")
	require.Contains(t, roleOut, "role-arn=arn:active:dd002491:role/reader")
	require.Contains(t, roleOut, "external-id=eid001")
}

// TestShowCreateCreatePlannerExternalTableMasksServiceArgumentCredentials runs
// the production arc this feature protects: production parser + CREATE planner
// (dmysql.Parse -> BuildPlan on the mock optimizer) -> stored TableDef
// (Createsql JSON, exactly what the DDL executor persists) -> production SHOW
// CREATE consumer. The rendered statement is the text mo-dump captures in a
// schema dump; asserting it carries no synthetic secrets verifies the
// dump-visible property without claiming a live S3 test.
func TestShowCreateCreatePlannerExternalTableMasksServiceArgumentCredentials(t *testing.T) {
	planExternal := func(t *testing.T, ddl string) (show string, def *plan.TableDef) {
		mock := NewMockOptimizer(false, newPlanTestProcess(t))
		statements, err := dmysql.Parse(mock.CurrentContext().GetContext(), ddl, 1)
		require.NoError(t, err)
		ddlPlan, err := BuildPlan(mock.CurrentContext(), statements[0], false)
		require.NoError(t, err)
		def = ddlPlan.Plan.(*plan.Plan_Ddl).Ddl.Definition.(*plan.DataDefinition_CreateTable).CreateTable.TableDef
		// Production storage: the planner marshals the extern options into the
		// SystemRelAttr_CreateSQL table property (build_ddl.go
		// json.Marshal(stmt.Param)); SHOW CREATE resolves that property into
		// TableDef.Createsql when the catalog reader materializes the def.
		// Mirror that resolution step, then run the real consumer arc.
		stored := ""
		for _, d := range def.Defs {
			if props, ok := d.Def.(*plan.TableDef_DefType_Properties); ok {
				for _, prop := range props.Properties.Properties {
					if prop.Key == catalog.SystemRelAttr_CreateSQL {
						stored = prop.Value
					}
				}
			}
		}
		require.NotEmpty(t, stored, "CREATE planner must persist extern options into the SystemRelAttr_CreateSQL property")
		def.Createsql = stored
		show, _, err = ConstructCreateTableSQL(&mock.ctxt, def, nil, false, nil)
		require.NoError(t, err)
		return show, def
	}

	t.Run("credential-bearing s3-opts path is masked", func(t *testing.T) {
		show, def := planExternal(t,
			"create external table mask_probe (a int) infile{'filepath'='s3-opts,endpoint=https://owned.invalid,region=us-east-1,bucket=b,key=CREDK0A2A1,secret=CREDS0A2A1:rows.csv','format'='csv'}")
		// the stored metadata still holds the credentials (restore-time
		// access); only the rendered SHOW CREATE must not.
		require.Contains(t, def.Createsql, "CREDK0A2A1")
		require.Contains(t, def.Createsql, "CREDS0A2A1")
		require.Contains(t, show, "CREATE EXTERNAL TABLE")
		require.Contains(t, show, "INFILE{'FILEPATH'='s3-opts,endpoint=https://owned.invalid,region=us-east-1,bucket=b,key=<redacted>,secret=<redacted>:rows.csv'")
		require.NotContains(t, show, "CREDK0A2A1")
		require.NotContains(t, show, "CREDS0A2A1")
	})

	t.Run("ordinary local path keeps exact bytes", func(t *testing.T) {
		show, _ := planExternal(t,
			"create external table mask_probe_local (a int) infile{'filepath'='/data/ext rows/rows.csv','format'='csv'}")
		require.Contains(t, show, "INFILE{'FILEPATH'='/data/ext rows/rows.csv'")
		require.NotContains(t, show, "<redacted>")
	})

	t.Run("short positional secret zone is masked fail-closed", func(t *testing.T) {
		show, def := planExternal(t,
			"create external table mask_probe_pos (a int) infile{'filepath'='s3,ep,region,bucket,PCRDSH0RT:rows.csv','format'='csv'}")
		require.Contains(t, def.Createsql, "PCRDSH0RT")
		require.Contains(t, show, "'FILEPATH'='s3,ep,region,bucket,<redacted>:rows.csv'")
		require.NotContains(t, show, "PCRDSH0RT")
	})

	t.Run("colonless s3-opts claim is masked fail-closed", func(t *testing.T) {
		show, def := planExternal(t,
			"create external table mask_probe_nocolon (a int) infile{'filepath'='s3-opts,key=PCRDco1,secret=PCRDco2','format'='csv'}")
		require.Contains(t, def.Createsql, "PCRDco1")
		require.Contains(t, def.Createsql, "PCRDco2")
		require.Contains(t, show, "'FILEPATH'='s3-opts,<redacted>'")
		require.NotContains(t, show, "PCRDco1")
		require.NotContains(t, show, "PCRDco2")
	})
}

// TestShowCreateExternalMasksFailClosedAndS3BranchFilepath covers the
// stored-metadata consumer arc (json.Marshal(ExternParam) ->
// TableDef.Createsql -> ConstructCreateTableSQL -> json.Unmarshal ->
// Init*Param -> render):
//   - a malformed but credential-bearing service section (admitted by CREATE:
//     InitInfileParam validates only option keys, never the path grammar) is
//     redacted fail-closed;
//   - an explicit-S3 table whose filepath is service-qualified with embedded
//     credentials is masked in the 'filepath' option too, and the branch's
//     existing explicit credential masking is retained.
func TestShowCreateExternalMasksFailClosedAndS3BranchFilepath(t *testing.T) {
	t.Run("malformed service-claimed section is fail-closed", func(t *testing.T) {
		show := buildTestShowCreateExternalTable(t, "mask_probe_broken", &tree.ExternParam{
			ExParamConst: tree.ExParamConst{
				ScanType: tree.INFILE,
				Option: []string{
					"filepath", `s3-opts,"endpoint=owned.invalid,key=CREDK0A3F1,secret=CREDS0A3F1:rows.csv`,
					"format", "csv",
				},
			},
		})
		require.Contains(t, show, "'FILEPATH'='s3-opts,<redacted>:rows.csv'")
		require.NotContains(t, show, "CREDK0A3F1")
		require.NotContains(t, show, "CREDS0A3F1")
	})

	t.Run("explicit S3 branch masks service-qualified filepath and explicit creds", func(t *testing.T) {
		show := buildTestShowCreateExternalTable(t, "mask_probe_s4", &tree.ExternParam{
			ExParamConst: tree.ExParamConst{
				ScanType: tree.S3,
				Option: []string{
					"filepath", "s3-opts,endpoint=owned.invalid,key=CREDK0A4S3,secret=CREDS0A4S3:x.csv",
					"endpoint", "owned.invalid",
					"bucket", "b",
					"access_key_id", "CREDAK0A4S3",
					"secret_access_key", "CREDAS0A4S3",
				},
			},
		})
		require.Contains(t, show, "key=<redacted>,secret=<redacted>:x.csv")
		for _, sentinel := range []string{"CREDK0A4S3", "CREDS0A4S3", "CREDAK0A4S3", "CREDAS0A4S3"} {
			require.NotContains(t, show, sentinel)
		}
	})
}
