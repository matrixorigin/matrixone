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

package upgrade

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Reuse only the subscriber fixture. Both sources are initialized here so the
// historical result cannot depend on which sibling subtests ran beforehand.
func testHistoricalSubscriptionCachedAlias(t *testing.T, ctx context.Context, sys, subscriber *sql.DB) {
	t.Helper()
	exec := func(db *sql.DB, query string) {
		t.Helper()
		_, err := db.ExecContext(ctx, query)
		require.NoError(t, err, query)
	}
	exec(sys, "create database view_description_cache_source")
	defer cleanupViewDescriptionSQL(t, sys, "drop database view_description_cache_source")
	exec(sys, "create table view_description_cache_source.src (x varchar(8))")
	exec(sys, "insert into view_description_cache_source.src values ('captured')")
	exec(sys, "create view view_description_cache_source.v as select x from view_description_cache_source.src")
	exec(sys, "create database view_description_cache_other")
	defer cleanupViewDescriptionSQL(t, sys, "drop database view_description_cache_other")
	exec(sys, "create table view_description_cache_other.src (x varchar(37))")
	exec(sys, "insert into view_description_cache_other.src values ('alternate')")
	exec(sys, "create view view_description_cache_other.v as select x from view_description_cache_other.src")
	exec(sys, "create publication view_description_cache_other_pub database view_description_cache_other account view_description_sub")
	defer cleanupViewDescriptionSQL(t, sys, "drop publication view_description_cache_other_pub")
	exec(sys, "create publication view_description_cache_pub database view_description_cache_source account view_description_sub")
	defer cleanupViewDescriptionSQL(t, sys, "drop publication view_description_cache_pub")
	exec(subscriber, "create database subscribed_cache from sys publication view_description_cache_pub")
	defer cleanupViewDescriptionSQL(t, subscriber, "drop database subscribed_cache")
	exec(subscriber, "create snapshot view_description_cache_history for account")
	defer cleanupViewDescriptionSQL(t, subscriber, "drop snapshot view_description_cache_history")

	conn, err := subscriber.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()
	const history = "select x from subscribed_cache.v {snapshot='view_description_cache_history'}"
	cached, err := conn.PrepareContext(ctx, history)
	require.NoError(t, err)
	defer cached.Close()
	_, err = conn.ExecContext(ctx, "prepare cached_alias_sql from 'select x from subscribed_cache.v {snapshot=''view_description_cache_history''}'")
	require.NoError(t, err)
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, err := conn.ExecContext(cleanupCtx, "deallocate prepare cached_alias_sql")
		require.NoError(t, err)
	}()
	check := func(allowed bool) {
		t.Helper()
		for _, query := range []func() *sql.Row{
			func() *sql.Row { return conn.QueryRowContext(ctx, history) },
			func() *sql.Row { return cached.QueryRowContext(ctx) },
			func() *sql.Row { return conn.QueryRowContext(ctx, "execute cached_alias_sql") },
		} {
			var value string
			err := query().Scan(&value)
			if allowed {
				require.NoError(t, err)
				require.Equal(t, "captured", value)
			} else {
				require.ErrorContains(t, err, "not allowed to subscribe the publication view_description_cache_pub")
			}
		}
		var count, width int
		require.NoError(t, conn.QueryRowContext(ctx,
			"select count(*), coalesce(max(character_maximum_length),0) from information_schema.columns "+
				"{snapshot='view_description_cache_history'} where table_schema='subscribed_cache' and table_name='v'").Scan(&count, &width))
		if allowed {
			require.Equal(t, 1, count)
			require.Equal(t, 8, width)
			func() {
				fresh, err := conn.PrepareContext(ctx, history)
				require.NoError(t, err)
				defer fresh.Close()
				var value string
				require.NoError(t, fresh.QueryRowContext(ctx).Scan(&value))
				require.Equal(t, "captured", value)
			}()
		} else {
			require.Zero(t, count)
			require.Zero(t, width)
		}
	}
	check(true)
	// DDL on another connection must exercise the cached handle's validation,
	// rather than relying on its own session DDL revision to force a rebuild.
	exec(subscriber, "drop database subscribed_cache")
	exec(subscriber, "create database subscribed_cache from sys publication view_description_cache_other_pub")
	var current string
	require.NoError(t, subscriber.QueryRowContext(ctx, "select x from subscribed_cache.v").Scan(&current))
	require.Equal(t, "alternate", current)
	exec(sys, "alter publication view_description_cache_other_pub account drop view_description_sub")
	require.ErrorContains(t, subscriber.QueryRowContext(ctx, "select x from subscribed_cache.v").Scan(&current),
		"not allowed to subscribe the publication view_description_cache_other_pub")
	check(true)
	exec(sys, "alter publication view_description_cache_pub account drop view_description_sub")
	restored := false
	defer func() {
		if !restored {
			cleanupViewDescriptionSQL(t, sys, "alter publication view_description_cache_pub account add view_description_sub")
		}
	}()
	check(false)
	exec(sys, "alter publication view_description_cache_pub account add view_description_sub")
	restored = true
	check(true)
}
