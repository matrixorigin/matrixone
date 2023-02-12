// Copyright 2023 Matrix Origin
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

package cmd

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
	"github.com/matrixorigin/matrixone/pkg/util/metric"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace"
	"github.com/spf13/cobra"
	"os"
	"path"
	"time"
)

var (
	tlExampleHost       string = "127.0.0.1"
	tlExamplePort       int    = 6001
	tlExampleUser       string = "dump"
	tlExamplePassword   string = "111"
	tlExampleDatabase   string = "test"
	tlExampleProvider   string = "s3"
	tlExampleEndpoint   string = "3.us-west-2.amazonaws.com"
	tlExampleRegion     string = "us-west-2"
	tlExampleBucket     string = "bucket_name"
	tlExampleKeyId      string = "I0AM0AWS0KEY0ID00000"
	tlExampleSecretKey  string = "0IAM0AWS0SECRET0KEY000000000000000000000"
	tlExamplePathPrefix string = "mo-data/etl"

	traceLogTimeout  int
	traceLogHost     string
	traceLogPort     int
	traceLogUser     string
	traceLogPassword string
	traceLogDatabase string
	// aws config
	traceLogProvider   string
	traceLogEndpoint   string
	traceLogRegion     string
	traceLogBucket     string
	traceLogKeyId      string
	traceLogSecretKey  string
	traceLogPathPrefix string
	traceLogShowSQL    bool
	traceLogExample    bool
	// use old schema, adapt old version mo
	traceLogOldSchema bool
)

func init() {
	rootCmd.AddCommand(traceLogCmd)
	// local args for try cmd
	traceLogCmd.Flags().IntVar(&traceLogTimeout, "timeout", 0, "access MO cluster timeout (seconds)")
	traceLogCmd.Flags().StringVar(&traceLogHost, "host", "127.0.0.1", "target MO cluster Host")
	traceLogCmd.Flags().IntVarP(&traceLogPort, "post", "P", 6001, "target MO cluster post")
	traceLogCmd.Flags().StringVarP(&traceLogUser, "user", "u", "test", "target MO cluster user (required)")
	traceLogCmd.Flags().StringVarP(&traceLogPassword, "password", "p", "", "target MO cluster password")
	traceLogCmd.Flags().StringVar(&traceLogDatabase, "db", "", "target MO cluster Database name (required)")
	traceLogCmd.Flags().StringVar(&traceLogProvider, "provider", "s3", "type of storage, val in [s3, minio]")
	traceLogCmd.Flags().StringVar(&traceLogEndpoint, "endpoint", "", "AWS endpoint (required)")
	traceLogCmd.Flags().StringVar(&traceLogRegion, "region", "", "AWS region (required)")
	traceLogCmd.Flags().StringVar(&traceLogBucket, "bucket", "", "AWS bucket (required)")
	traceLogCmd.Flags().StringVar(&traceLogKeyId, "access-key-id", "", "AWS bucket (required)")
	traceLogCmd.Flags().StringVar(&traceLogSecretKey, "secret-access-key", "", "AWS bucket (required)")
	traceLogCmd.Flags().StringVar(&traceLogPathPrefix, "path-prefix", "", "AWS bucket (required)")
	traceLogCmd.Flags().BoolVar(&traceLogShowSQL, "show", false, "show all sql will exec")
	traceLogCmd.Flags().BoolVar(&traceLogExample, "example", false, "show cmd example")
	traceLogCmd.Flags().BoolVar(&traceLogOldSchema, "old-schema", false, "use old schema. you can see the diffs with '--show' flag")
}

var traceLogCmd = &cobra.Command{
	Use:   "tracelog",
	Short: "Help to create external tables in target mo cluster, that helps to access others mo cluster's trace/log/metric info.",
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := traceLogCmdFunc(); err != nil {
			return err
		}
		return nil
	},
}

const connTimeout = 10 * time.Second

func traceLogCmdFunc() error {
	var err error
	var ctx = context.Background()
	if traceLogExample {
		return printExtExample()
	}

	err = checkRequireParam()
	if err != nil {
		return err
	}

	if traceLogShowSQL {
		return printStatement(ctx)
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?readTimeout=%[6]s&writeTimeout=%[6]s&timeout=%[6]s",
		traceLogUser, traceLogPassword, traceLogHost, traceLogPort, traceLogPassword, time.Duration(traceLogTimeout)*time.Second)
	conn, err := sql.Open("mysql", dsn) // Open doesn't open a connection. Validate DSN data:
	if err != nil {
		return err
	}
	ch := make(chan error)
	go func() {
		err = conn.Ping() // Before use, we must ping to validate DSN data:
		ch <- err
	}()

	select {
	case err = <-ch:
	case <-time.After(connTimeout):
		err = moerr.NewInternalError(ctx, "connect to %s timeout", dsn)
	}
	if err != nil {
		return err
	}

	stmts := getStatements(ctx)
	for _, sql := range stmts {
		_, err = conn.ExecContext(ctx, sql)
		if err != nil {
			fmt.Fprintf(os.Stderr, "exec sql: %s\n", sql)
			return err
		}
	}

	return nil
}

func checkRequireParam() error {

	// check aws config
	if traceLogProvider == "s3" {
		if len(traceLogRegion) == 0 {
			return fmt.Errorf("missing flag --region")
		}
	}

	if len(traceLogEndpoint) == 0 {
		return fmt.Errorf("missing flag --endpoint")
	}
	if len(traceLogBucket) == 0 {
		return fmt.Errorf("missing flag --bucket")
	}
	if len(traceLogKeyId) == 0 {
		return fmt.Errorf("missing flag --access-key-id")
	}
	if len(traceLogSecretKey) == 0 {
		return fmt.Errorf("missing flag --secret-access-key")
	}
	if len(traceLogPathPrefix) == 0 {
		return fmt.Errorf("missing flag --path-prefix")
	}

	return nil
}

func printExtExample() error {
	bin := os.Args[0]
	fmt.Printf(`example:
  %[1]s external --host "%s" -P %d -u%s -p "%s" --db "%s" \
	--endpoint "%s" --region "%s" --bucket "%s" \
	--access-key-id "%s" --secret-access-key "%s" \
	--path-prefix "%s" --provider "%s"
`, bin, tlExampleHost, tlExamplePort, tlExampleUser, tlExamplePassword, tlExampleDatabase,
		tlExampleEndpoint, tlExampleRegion, tlExampleBucket,
		tlExampleKeyId, tlExampleSecretKey, tlExamplePathPrefix, tlExampleProvider)
	return nil
}

func getStatements(ctx context.Context) (statements []string) {
	if traceLogOldSchema {
		// adapt old version
		motrace.SingleStatementTable.Columns = motrace.SingleStatementTable.Columns[:21]
		motrace.SingleRowLogTable.Columns = motrace.SingleRowLogTable.Columns[:20]
		// remove span_kind column
		for _, view := range gViews {
			newCols := make([]table.Column, 0, len(view.Columns))
			for _, col := range view.Columns {
				if col.Name == "span_kind" {
					continue
				}
				newCols = append(newCols, col)
			}
			view.Columns = newCols
		}
	}
	// build path
	pBuilder := &traceLogPathBuilder{
		AccountDatePathBuilder: table.NewAccountDatePathBuilder(),
	}
	statements = append(statements, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`;", traceLogDatabase))
	for _, tbl := range gTables {
		tbl.PathBuilder = pBuilder
		tbl.Database = traceLogDatabase
		tbl.TableOptions = &traceLogTableOptions{
			DbName:  tbl.Database,
			TblName: tbl.Table,
			Account: tbl.Account,
		}
		statements = append(statements, tbl.ToCreateSql(ctx, true))
	}
	for _, view := range gViews {
		view.Database = traceLogDatabase
		statements = append(statements, view.ToCreateSql(ctx, true))
	}

	return statements
}

func printStatement(ctx context.Context) error {
	statements := getStatements(ctx)
	for _, sql := range statements {
		fmt.Println(sql)
	}
	return nil
}

var gTables = []*table.Table{metric.SingleMetricTable, motrace.SingleStatementTable, motrace.SingleRowLogTable}
var gViews = []*table.View{motrace.LogView, motrace.SpanView, motrace.ErrorView}

var _ table.PathBuilder = (*traceLogPathBuilder)(nil)

type traceLogPathBuilder struct {
	*table.AccountDatePathBuilder
}

func (e *traceLogPathBuilder) BuildETLPath(db, name, account string) string {
	return path.Join(traceLogPathPrefix, e.AccountDatePathBuilder.BuildETLPath(db, name, account))
}

func (e *traceLogPathBuilder) GetName() string {
	return "traceLogPathBuilder"
}

var _ table.TableOptions = (*traceLogTableOptions)(nil)

type traceLogTableOptions struct {
	DbName  string
	TblName string
	Account string
}

func (o *traceLogTableOptions) FormatDdl(ddl string) string {
	panic("not implement")
}

func (o *traceLogTableOptions) GetCreateOptions() string {
	return "EXTERNAL "
}

func (o *traceLogTableOptions) GetTableOptions(builder table.PathBuilder) string {
	return fmt.Sprintf(`url s3option { "endpoint"=%q, "access_key_id"=%q, "secret_access_key"=%q, "region"=%q, "bucket"=%q, "filepath" = %q, "provider"=%q, "compression" = "none" } FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 0 lines`,
		traceLogEndpoint, traceLogKeyId, traceLogSecretKey, traceLogRegion, traceLogBucket,
		builder.BuildETLPath(o.DbName, o.TblName, o.Account),
		traceLogProvider,
	)
}
