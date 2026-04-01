// Copyright 2021 Matrix Origin
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

package frontend

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

const connectionStatusActive = "active"

var (
	connectionSQLLiteralEscaper = strings.NewReplacer(
		"\\", "\\\\",
		"\n", "\\n",
		"\x00", "\\0",
		"\r", "\\r",
		"\b", "\\b",
		string(rune(26)), "\\Z",
		"\t", "\\t",
		"'", "''",
	)

	connectionTypeAliases = map[string]string{
		"mysql":      "mysql",
		"oracle":     "oracle",
		"postgresql": "postgresql",
		"postgres":   "postgresql",
		"pg":         "postgresql",
	}

	connectionSensitiveOptions = map[string]struct{}{
		"password":      {},
		"passwd":        {},
		"secret_key":    {},
		"access_key":    {},
		"session_token": {},
		"token":         {},
		"private_key":   {},
	}

	connectionCommonRequiredOptions = []string{"host", "port", "user", "password"}

	showCreateConnectionCols = []Column{
		&MysqlColumn{
			ColumnImpl: ColumnImpl{
				name:       "Connection",
				columnType: defines.MYSQL_TYPE_VARCHAR,
			},
		},
		&MysqlColumn{
			ColumnImpl: ColumnImpl{
				name:       "Create Connection",
				columnType: defines.MYSQL_TYPE_VARCHAR,
			},
		},
	}
)

type connectionMetadata struct {
	name        string
	typ         string
	optionsJSON string
}

func doCreateConnection(ctx context.Context, ses *Session, cs *tree.CreateConnection) (err error) {
	if err = doCheckRole(ctx, ses); err != nil {
		return err
	}
	if err = inputNameIsInvalid(ctx, string(cs.Name)); err != nil {
		return err
	}

	connType, err := normalizeConnectionType(ctx, cs.Type)
	if err != nil {
		return err
	}
	options, err := normalizeConnectionOptions(ctx, cs.Options)
	if err != nil {
		return err
	}
	if err = validateConnectionOptions(ctx, connType, options); err != nil {
		return err
	}

	tenantInfo := ses.GetTenantInfo()
	if tenantInfo == nil {
		return moerr.NewInternalError(ctx, "missing tenant info")
	}

	optionsJSON, err := json.Marshal(options)
	if err != nil {
		return err
	}

	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	if err = bh.Exec(ctx, "begin;"); err != nil {
		return err
	}
	defer func() {
		err = finishTxn(ctx, bh, err)
	}()

	exists, err := checkConnectionExistOrNot(ctx, bh, string(cs.Name))
	if err != nil {
		return err
	}
	if exists {
		if cs.IfNotExists {
			return nil
		}
		return moerr.NewInternalErrorf(ctx, "the connection %s exists", cs.Name)
	}

	sql, err := getSqlForInsertIntoMoConnections(
		ctx,
		string(cs.Name),
		connType,
		string(optionsJSON),
		connectionStatusActive,
		uint64(tenantInfo.GetDefaultRoleID()),
		uint64(tenantInfo.GetUserID()),
		uint64(tenantInfo.GetTenantID()),
		types.CurrentTimestamp().String2(time.UTC, 0),
		"",
	)
	if err != nil {
		return err
	}
	return bh.Exec(ctx, sql)
}

func doDropConnection(ctx context.Context, ses *Session, ds *tree.DropConnection) (err error) {
	if err = doCheckRole(ctx, ses); err != nil {
		return err
	}
	if err = inputNameIsInvalid(ctx, string(ds.Name)); err != nil {
		return err
	}

	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	if err = bh.Exec(ctx, "begin;"); err != nil {
		return err
	}
	defer func() {
		err = finishTxn(ctx, bh, err)
	}()

	exists, err := checkConnectionExistOrNot(ctx, bh, string(ds.Name))
	if err != nil {
		return err
	}
	if !exists {
		if ds.IfExists {
			return nil
		}
		return moerr.NewInternalErrorf(ctx, "the connection %s does not exist", ds.Name)
	}

	return bh.Exec(ctx, getSqlForDropConnection(string(ds.Name)))
}

func doShowCreateConnection(ctx context.Context, ses *Session, sc *tree.ShowCreateConnection) (err error) {
	if err = doCheckRole(ctx, ses); err != nil {
		return err
	}
	if err = inputNameIsInvalid(ctx, sc.Name); err != nil {
		return err
	}

	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	meta, err := loadConnectionMetadata(ctx, bh, sc.Name)
	if err != nil {
		return err
	}
	if meta == nil {
		return moerr.NewInternalErrorf(ctx, "the connection %s does not exist", sc.Name)
	}

	options, err := decodeConnectionOptions(meta.optionsJSON)
	if err != nil {
		return err
	}

	mrs := ses.GetMysqlResultSet()
	for _, col := range showCreateConnectionCols {
		mrs.AddColumn(col)
	}
	mrs.AddRow([]interface{}{
		meta.name,
		buildShowCreateConnectionSQL(meta.name, meta.typ, options),
	})
	return trySaveQueryResult(ctx, ses, mrs)
}

func normalizeConnectionType(ctx context.Context, typ string) (string, error) {
	normalized := strings.ToLower(strings.TrimSpace(typ))
	if normalized == "" {
		return "", moerr.NewBadConfig(ctx, "connection type is required")
	}
	aliased, ok := connectionTypeAliases[normalized]
	if !ok {
		return "", moerr.NewBadConfig(ctx, fmt.Sprintf("unsupported connection type %q", typ))
	}
	return aliased, nil
}

func normalizeConnectionOptions(ctx context.Context, raw []*tree.ConnectionOption) (map[string]string, error) {
	options := make(map[string]string, len(raw))
	for _, opt := range raw {
		if opt == nil {
			return nil, moerr.NewBadConfig(ctx, "connection option is nil")
		}
		key := strings.ToLower(strings.TrimSpace(string(opt.Key)))
		if key == "" {
			return nil, moerr.NewBadConfig(ctx, "connection option name is empty")
		}
		if _, exists := options[key]; exists {
			return nil, moerr.NewBadConfig(ctx, fmt.Sprintf("duplicate connection option %q", key))
		}
		options[key] = opt.Value
	}
	return options, nil
}

func validateConnectionOptions(ctx context.Context, typ string, options map[string]string) error {
	for _, key := range connectionCommonRequiredOptions {
		value, ok := options[key]
		if !ok || strings.TrimSpace(value) == "" {
			return moerr.NewBadConfig(ctx, fmt.Sprintf("connection option %q is required", key))
		}
	}

	port, err := strconv.Atoi(strings.TrimSpace(options["port"]))
	if err != nil || port <= 0 {
		return moerr.NewBadConfig(ctx, "connection option \"port\" must be a positive integer")
	}

	if typ == "oracle" {
		if strings.TrimSpace(options["service_name"]) == "" && strings.TrimSpace(options["sid"]) == "" {
			return moerr.NewBadConfig(ctx, "oracle connection requires service_name or sid")
		}
	}
	return nil
}

func checkConnectionExistOrNot(ctx context.Context, bh BackgroundExec, connectionName string) (bool, error) {
	bh.ClearExecResultSet()
	if err := bh.Exec(ctx, getSqlForCheckConnection(connectionName)); err != nil {
		return false, err
	}

	erArray, err := getResultSet(ctx, bh)
	if err != nil {
		return false, err
	}
	return execResultArrayHasData(erArray), nil
}

func loadConnectionMetadata(ctx context.Context, bh BackgroundExec, connectionName string) (*connectionMetadata, error) {
	bh.ClearExecResultSet()
	if err := bh.Exec(ctx, getSqlForGetConnection(connectionName)); err != nil {
		return nil, err
	}

	erArray, err := getResultSet(ctx, bh)
	if err != nil {
		return nil, err
	}
	if !execResultArrayHasData(erArray) {
		return nil, nil
	}

	typ, err := erArray[0].GetString(ctx, 0, 0)
	if err != nil {
		return nil, err
	}
	optionsJSON, err := erArray[0].GetString(ctx, 0, 1)
	if err != nil {
		return nil, err
	}

	return &connectionMetadata{
		name:        connectionName,
		typ:         typ,
		optionsJSON: optionsJSON,
	}, nil
}

func decodeConnectionOptions(raw string) (map[string]string, error) {
	if raw == "" {
		return map[string]string{}, nil
	}

	options := make(map[string]string)
	if err := json.Unmarshal([]byte(raw), &options); err != nil {
		return nil, err
	}
	return options, nil
}

func buildShowCreateConnectionSQL(name, typ string, options map[string]string) string {
	maskedOptions := maskConnectionOptions(options)
	keys := make([]string, 0, len(maskedOptions))
	for key := range maskedOptions {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	formattedOptions := make([]*tree.ConnectionOption, 0, len(keys))
	for _, key := range keys {
		formattedOptions = append(formattedOptions, &tree.ConnectionOption{
			Key:   tree.Identifier(key),
			Value: maskedOptions[key],
		})
	}

	stmt := &tree.CreateConnection{
		Name:    tree.Identifier(name),
		Type:    typ,
		Options: formattedOptions,
	}
	return tree.String(stmt, dialect.MYSQL)
}

func maskConnectionOptions(options map[string]string) map[string]string {
	masked := make(map[string]string, len(options))
	for key, value := range options {
		if _, ok := connectionSensitiveOptions[strings.ToLower(key)]; ok {
			masked[key] = "***"
			continue
		}
		masked[key] = value
	}
	return masked
}

func getSqlForCheckConnection(connectionName string) string {
	return fmt.Sprintf(
		"select connection_id from %s.%s where connection_name = '%s' order by connection_id;",
		catalog.MO_CATALOG,
		catalog.MO_CONNECTIONS,
		escapeConnectionSQLLiteral(connectionName),
	)
}

func getSqlForGetConnection(connectionName string) string {
	return fmt.Sprintf(
		"select connection_type, connection_options from %s.%s where connection_name = '%s' order by connection_id;",
		catalog.MO_CATALOG,
		catalog.MO_CONNECTIONS,
		escapeConnectionSQLLiteral(connectionName),
	)
}

func getSqlForInsertIntoMoConnections(
	ctx context.Context,
	connectionName string,
	connectionType string,
	connectionOptions string,
	connectionStatus string,
	owner uint64,
	creator uint64,
	accountID uint64,
	createdTime string,
	comment string,
) (string, error) {
	if err := inputNameIsInvalid(ctx, connectionName); err != nil {
		return "", err
	}

	return fmt.Sprintf(
		"insert into %s.%s(connection_name, connection_type, connection_options, connection_status, owner, creator, account_id, created_time, comment) values ('%s', '%s', '%s', '%s', %d, %d, %d, '%s', '%s');",
		catalog.MO_CATALOG,
		catalog.MO_CONNECTIONS,
		escapeConnectionSQLLiteral(connectionName),
		escapeConnectionSQLLiteral(connectionType),
		escapeConnectionSQLLiteral(connectionOptions),
		escapeConnectionSQLLiteral(connectionStatus),
		owner,
		creator,
		accountID,
		escapeConnectionSQLLiteral(createdTime),
		escapeConnectionSQLLiteral(comment),
	), nil
}

func getSqlForDropConnection(connectionName string) string {
	return fmt.Sprintf(
		"delete from %s.%s where connection_name = '%s' order by connection_id;",
		catalog.MO_CATALOG,
		catalog.MO_CONNECTIONS,
		escapeConnectionSQLLiteral(connectionName),
	)
}

func escapeConnectionSQLLiteral(value string) string {
	return connectionSQLLiteralEscaper.Replace(value)
}
