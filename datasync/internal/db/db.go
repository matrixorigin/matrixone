package db

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
)

type Endpoint struct {
	Host     string
	Port     int
	User     string
	Password string
}

type Table struct {
	Name string
	Kind string
}

type Client struct {
	db *sql.DB
}

func DSN(ep Endpoint, database string) string {
	cfg := mysql.NewConfig()
	cfg.User = strings.ReplaceAll(ep.User, ":", "#")
	cfg.Passwd = ep.Password
	cfg.Net = "tcp"
	cfg.Addr = fmt.Sprintf("%s:%d", ep.Host, ep.Port)
	cfg.DBName = database
	cfg.AllowAllFiles = true
	cfg.MultiStatements = true
	cfg.ParseTime = true
	return cfg.FormatDSN()
}

func Open(ctx context.Context, ep Endpoint, database string) (*Client, error) {
	conn, err := sql.Open("mysql", DSN(ep, database))
	if err != nil {
		return nil, err
	}

	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := conn.PingContext(pingCtx); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return &Client{db: conn}, nil
}

func (c *Client) Close() error {
	if c == nil || c.db == nil {
		return nil
	}
	return c.db.Close()
}

func (c *Client) ListOrdinaryTables(ctx context.Context, database string) ([]Table, error) {
	rows, err := c.db.QueryContext(ctx, "select relname, relkind from mo_catalog.mo_tables where reldatabase = ? and relkind = 'r'", database)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []Table
	for rows.Next() {
		var tbl Table
		if err := rows.Scan(&tbl.Name, &tbl.Kind); err != nil {
			return nil, err
		}
		if strings.HasPrefix(tbl.Name, "__mo_") || strings.HasPrefix(tbl.Name, "%!%") {
			continue
		}
		tables = append(tables, tbl)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return tables, nil
}

func (c *Client) CountRows(ctx context.Context, database, table string) (int64, error) {
	query := fmt.Sprintf("select count(*) from %s.%s", QuoteIdent(database), QuoteIdent(table))
	var count int64
	if err := c.db.QueryRowContext(ctx, query).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func (c *Client) EnsureDatabase(ctx context.Context, database string) error {
	_, err := c.db.ExecContext(ctx, "create database if not exists "+QuoteIdent(database))
	return err
}

func QuoteIdent(s string) string {
	return "`" + strings.ReplaceAll(s, "`", "``") + "`"
}
