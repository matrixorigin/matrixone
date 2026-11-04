// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package issues

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/matrixorigin/matrixone/pkg/embed"
)

func TestIssue29856KafkaJSONProjection(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()

		broker, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "issue29856_jsonl", "issue29856_csv"))
		require.NoError(t, err)
		defer broker.Close()
		addr := broker.ListenAddrs()[0]
		producer, err := kgo.NewClient(kgo.SeedBrokers(addr))
		require.NoError(t, err)
		defer producer.Close()
		for _, record := range []*kgo.Record{
			{Topic: "issue29856_jsonl", Value: []byte(`{"event_id":"valid-event","delta":1}`)},
			{Topic: "issue29856_csv", Value: []byte("valid-event,1")},
		} {
			require.NoError(t, producer.ProduceSync(ctx, record).FirstErr())
		}

		port := cn.GetServiceConfig().CN.Frontend.Port
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		_, err = conn.ExecContext(ctx, "drop database if exists issue_29856")
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "create database issue_29856")
		require.NoError(t, err)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cleanupCancel()
			_, _ = conn.ExecContext(cleanupCtx, "drop database if exists issue_29856")
		}()
		for _, format := range []string{"jsonl", "csv"} {
			statement := fmt.Sprintf(`create external table issue_29856.%s (
				event_id varchar(96), delta bigint
			) engine=kafka with ('brokers'='%s', 'topic'='issue29856_%s',
				'partition'='0', 'autocommit'='false', 'group'='issue29856_%s', 'format'='%s')`,
				format, addr, format, format, format)
			_, err = conn.ExecContext(ctx, statement)
			require.NoError(t, err)
		}

		read := func(projection, format string, values ...any) {
			t.Helper()
			query := fmt.Sprintf(`select %s from issue_29856.%s
				where __mo_read_start_id=-1 and __mo_read_size=1 and __mo_read_timeout=2`, projection, format)
			require.NoError(t, conn.QueryRowContext(ctx, query).Scan(values...))
		}
		_, err = conn.ExecContext(ctx, "set session sql_mode='STRICT_TRANS_TABLES'")
		require.NoError(t, err)
		var eventID string
		var delta, offset int64
		read("*", "jsonl", &eventID, &delta)
		require.Equal(t, "valid-event", eventID)
		require.Equal(t, int64(1), delta)
		read("event_id", "jsonl", &eventID)
		require.Equal(t, "valid-event", eventID)
		read("delta", "jsonl", &delta)
		require.Equal(t, int64(1), delta)
		read("__mo_message_id", "jsonl", &offset)
		require.Equal(t, int64(0), offset)
		read("__mo_message_id,event_id", "jsonl", &offset, &eventID)
		require.Equal(t, int64(0), offset)
		require.Equal(t, "valid-event", eventID)
		read("__mo_message_id", "csv", &offset)
		require.Equal(t, int64(0), offset)

		_, err = conn.ExecContext(ctx, "set session sql_mode=''")
		require.NoError(t, err)
		read("__mo_message_id", "jsonl", &offset)
		require.Equal(t, int64(0), offset)
		read("event_id", "jsonl", &eventID)
		require.Equal(t, "valid-event", eventID)
	})
}
