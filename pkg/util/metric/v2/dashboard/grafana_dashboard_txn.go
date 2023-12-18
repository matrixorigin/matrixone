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

package dashboard

import (
	"context"

	"github.com/K-Phoen/grabana/axis"
	"github.com/K-Phoen/grabana/dashboard"
)

func (c *DashboardCreator) initTxnDashboard() error {
	folder, err := c.createFolder(moFolderName)
	if err != nil {
		return err
	}

	build, err := dashboard.New(
		"Txn Metrics",
		c.withRowOptions(
			c.initTxnOverviewRow(),
			c.initTxnDurationRow(),
			c.initTxnCommitDurationRow(),
			c.initTxnLockDurationRow(),
			c.initTxnUnlockTablesRow(),
			c.initTxnLockWaitersRow(),
			c.initTxnStatementDurationRow(),
			c.initTxnStatementsCountRow(),
			c.initTxnTableRangesRow(),
			c.initTxnReaderDurationRow(),
			c.initTxnMpoolRow(),
			c.initTxnOnPrepareWALRow(),
			c.initTxnBeforeCommitRow(),
			c.initTxnDequeuePreparedRow(),
			c.initTxnDequeuePreparingRow(),
			c.initTxnRangesLoadedObjectMetaRow(),
			c.initTxnShowAccountsRow(),
			c.initCNCommittedObjectQuantityRow(),
		)...)
	if err != nil {
		return err
	}
	_, err = c.cli.UpsertDashboard(context.Background(), folder, build)
	return err
}

func (c *DashboardCreator) initCNCommittedObjectQuantityRow() dashboard.Option {
	return dashboard.Row(
		"Quantity of Object Location The CN Have Committed to TN",
		c.withGraph(
			"meta location",
			6,
			`sum(`+c.getMetricWithFilter("mo_txn_cn_committed_location_quantity_size", `type="meta_location"`)+`)`,
			""),

		c.withGraph(
			"delta location",
			6,
			`sum(`+c.getMetricWithFilter("mo_txn_cn_committed_location_quantity_size", `type="delta_location"`)+`)`,
			""),
	)
}

func (c *DashboardCreator) initTxnOverviewRow() dashboard.Option {
	return dashboard.Row(
		"Txn overview",
		c.withMultiGraph(
			"Txn requests",
			3,
			[]string{
				`sum(rate(` + c.getMetricWithFilter("mo_txn_total", `type="user"`) + `[$interval]))`,
				`sum(rate(` + c.getMetricWithFilter("mo_txn_total", `type="internal"`) + `[$interval]))`,
				`sum(rate(` + c.getMetricWithFilter("mo_txn_total", `type="leak"`) + `[$interval]))`,
				`sum(rate(` + c.getMetricWithFilter("mo_txn_statement_total", `type="total"`) + `[$interval]))`,
				`sum(rate(` + c.getMetricWithFilter("mo_txn_statement_total", `type="retry"`) + `[$interval]))`,
				`sum(rate(` + c.getMetricWithFilter("mo_txn_lock_total", `type="total"`) + `[$interval]))`,
				`sum(rate(` + c.getMetricWithFilter("mo_txn_lock_total", `type="local"`) + `[$interval]))`,
				`sum(rate(` + c.getMetricWithFilter("mo_txn_lock_total", `type="remote"`) + `[$interval]))`,
			},
			[]string{
				"user-txn",
				"internal-txn",
				"leak",
				"statement",
				"statement-retry",
				"lock",
				"local-lock",
				"remote-lock",
			}),

		c.withMultiGraph(
			"Commit requests",
			3,
			[]string{
				`sum(rate(` + c.getMetricWithFilter("mo_txn_commit_total", `type="cn"`) + `[$interval]))`,
				`sum(rate(` + c.getMetricWithFilter("mo_txn_commit_total", `type="tn-receive"`) + `[$interval]))`,
				`sum(rate(` + c.getMetricWithFilter("mo_txn_commit_total", `type="tn-handle"`) + `[$interval]))`,
			},
			[]string{
				"cn",
				"tn-receive",
				"tn-handle",
			}),

		c.withGraph(
			"Rollback requests",
			3,
			`sum(rate(`+c.getMetricWithFilter("mo_txn_rollback_total", "")+`[$interval])) by (`+c.by+`)`,
			"{{ "+c.by+" }}"),

		c.withMultiGraph(
			"Txn Queue Status",
			3,
			[]string{
				`sum(` + c.getMetricWithFilter("mo_txn_queue_size", `type="active"`) + `)`,
				`sum(` + c.getMetricWithFilter("mo_txn_queue_size", `type="wait-active"`) + `)`,
				`sum(` + c.getMetricWithFilter("mo_txn_queue_size", `type="commit"`) + `)`,
				`sum(` + c.getMetricWithFilter("mo_txn_queue_size", `type="lock-rpc"`) + `)`,
			},
			[]string{
				"active",
				"wait-active",
				"commit",
				"lock-rpc",
			}),
	)
}

func (c *DashboardCreator) initTxnDurationRow() dashboard.Option {
	return dashboard.Row(
		"Txn create duration",
		c.getMultiHistogram(
			[]string{
				c.getMetricWithFilter(`mo_txn_life_duration_seconds_bucket`, ``),
				c.getMetricWithFilter(`mo_txn_create_duration_seconds_bucket`, `type="total"`),
				c.getMetricWithFilter(`mo_txn_create_duration_seconds_bucket`, `type="wait-active"`),
				c.getMetricWithFilter(`mo_txn_create_duration_seconds_bucket`, `type="determine-snapshot"`),
			},
			[]string{
				"life",
				"create",
				"wait-active",
				"determine-snapshot",
			},
			[]float64{0.50, 0.8, 0.90, 0.99},
			[]float32{3, 3, 3, 3},
			axis.Unit("s"),
			axis.Min(0))...,
	)
}

func (c *DashboardCreator) initTxnCommitDurationRow() dashboard.Option {
	return dashboard.Row(
		"Txn CN commit duration",
		c.getMultiHistogram(
			[]string{
				c.getMetricWithFilter("mo_txn_commit_duration_seconds_bucket", `type="cn"`),
				c.getMetricWithFilter(`mo_txn_commit_duration_seconds_bucket`, `type="cn-send"`),
				c.getMetricWithFilter(`mo_txn_commit_duration_seconds_bucket`, `type="cn-resp"`),
				c.getMetricWithFilter(`mo_txn_commit_duration_seconds_bucket`, `type="cn-wait-logtail"`),
				c.getMetricWithFilter(`mo_txn_commit_duration_seconds_bucket`, `type="tn"`),
			},
			[]string{
				"total",
				"send",
				"response",
				"logtail-applied",
				"tn",
			},
			[]float64{0.50, 0.8, 0.90, 0.99},
			[]float32{3, 3, 3, 3},
			axis.Unit("s"),
			axis.Min(0))...,
	)
}

func (c *DashboardCreator) initTxnOnPrepareWALRow() dashboard.Option {
	return dashboard.Row(
		"txn on prepare wal duration",
		c.getMultiHistogram(
			[]string{
				c.getMetricWithFilter("mo_txn_tn_side_duration_seconds_bucket", `step="on_prepare_wal_total"`),
				c.getMetricWithFilter("mo_txn_tn_side_duration_seconds_bucket", `step="on_prepare_wal_prepare_wal"`),
				c.getMetricWithFilter("mo_txn_tn_side_duration_seconds_bucket", `step="on_prepare_wal_end_prepare"`),
				c.getMetricWithFilter("mo_txn_tn_side_duration_seconds_bucket", `step="on_prepare_wal_flush_queue"`),
			},
			[]string{
				"total",
				"prepare wal",
				"end prepare",
				"enqueue flush",
			},
			[]float64{0.80, 0.90, 0.95, 0.99},
			[]float32{3, 3, 3, 3},
			axis.Unit("s"),
			axis.Min(0))...,
	)
}

func (c *DashboardCreator) initTxnDequeuePreparingRow() dashboard.Option {
	return dashboard.Row(
		"txn dequeue preparing duration",
		c.getHistogram(
			"txn dequeue preparing duration",
			c.getMetricWithFilter("mo_txn_tn_side_duration_seconds_bucket", `step="dequeue_preparing"`),
			[]float64{0.50, 0.8, 0.90, 0.99},
			12,
			axis.Unit("s"),
			axis.Min(0)),
	)
}

func (c *DashboardCreator) initTxnDequeuePreparedRow() dashboard.Option {
	return dashboard.Row(
		"txn dequeue prepared duration",
		c.getHistogram(
			"txn dequeue prepared duration",
			c.getMetricWithFilter("mo_txn_tn_side_duration_seconds_bucket", `step="dequeue_prepared"`),
			[]float64{0.50, 0.8, 0.90, 0.99},
			12,
			axis.Unit("s"),
			axis.Min(0)),
	)
}

func (c *DashboardCreator) initTxnBeforeCommitRow() dashboard.Option {
	return dashboard.Row(
		"txn handle commit but before txn.commit duration",
		c.getHistogram(
			"txn handle commit but before txn.commit duration",
			c.getMetricWithFilter("mo_txn_tn_side_duration_seconds_bucket", `step="before_txn_commit"`),
			[]float64{0.50, 0.8, 0.90, 0.99},
			12,
			axis.Unit("s"),
			axis.Min(0)),
	)
}

func (c *DashboardCreator) initTxnStatementDurationRow() dashboard.Option {
	return dashboard.Row(
		"Txn statement duration",
		c.getMultiHistogram(
			[]string{
				c.getMetricWithFilter(`mo_txn_statement_duration_seconds_bucket`, `type="execute"`),
				c.getMetricWithFilter(`mo_txn_statement_duration_seconds_bucket`, `type="execute-latency"`),
				c.getMetricWithFilter(`mo_txn_statement_duration_seconds_bucket`, `type="build-plan"`),
				c.getMetricWithFilter(`mo_txn_statement_duration_seconds_bucket`, `type="compile"`),
			},
			[]string{
				"execute",
				"execute-latency",
				"build-plan",
				"compile",
			},
			[]float64{0.50, 0.8, 0.90, 0.99},
			[]float32{3, 3, 3, 3},
			axis.Unit("s"),
			axis.Min(0))...,
	)
}

func (c *DashboardCreator) initTxnStatementsCountRow() dashboard.Option {
	return dashboard.Row(
		"Txn statements count",
		c.getMultiHistogram(
			[]string{
				c.getMetricWithFilter(`mo_txn_life_statements_total_bucket`, ``),
			},
			[]string{
				"statements/txn",
			},
			[]float64{0.50, 0.8, 0.90, 0.99},
			[]float32{3, 3, 3, 3})...,
	)
}

func (c *DashboardCreator) initTxnRangesLoadedObjectMetaRow() dashboard.Option {
	return dashboard.Row(
		"Txn Ranges Loaded Object Meta",
		c.withGraph(
			"Txn Ranges Loaded Object Meta",
			12,
			`sum(increase(`+c.getMetricWithFilter("mo_txn_ranges_loaded_object_meta_total", "")+`[$interval])) by (`+c.by+`, type)`,
			"{{ "+c.by+"-type }}"),
	)
}

func (c *DashboardCreator) initTxnShowAccountsRow() dashboard.Option {
	return dashboard.Row(
		"Show Accounts Duration",
		c.getHistogram(
			"Show Accounts Duration",
			c.getMetricWithFilter(`mo_txn_show_accounts_duration_seconds_bucket`, ``),
			[]float64{0.50, 0.8, 0.90, 0.99},
			12,
			axis.Unit("s"),
			axis.Min(0)),
	)
}

func (c *DashboardCreator) initTxnTableRangesRow() dashboard.Option {
	return dashboard.Row(
		"Txn table ranges",
		c.getHistogram(
			"Txn table ranges duration",
			c.getMetricWithFilter(`mo_txn_ranges_duration_seconds_bucket`, ``),
			[]float64{0.50, 0.8, 0.90, 0.99},
			6,
			axis.Unit("s"),
			axis.Min(0)),

		c.getHistogram(
			"Txn table ranges count",
			c.getMetricWithFilter(`mo_txn_ranges_duration_size_bucket`, ``),
			[]float64{0.50, 0.8, 0.90, 0.99},
			6),
	)
}

func (c *DashboardCreator) initTxnLockDurationRow() dashboard.Option {
	return dashboard.Row(
		"Txn lock duration",
		c.getMultiHistogram(
			[]string{
				c.getMetricWithFilter(`mo_txn_lock_duration_seconds_bucket`, `type="acquire"`),
				c.getMetricWithFilter(`mo_txn_lock_duration_seconds_bucket`, `type="acquire-wait"`),
				c.getMetricWithFilter(`mo_txn_unlock_duration_seconds_bucket`, `type="total"`),
				c.getMetricWithFilter(`mo_txn_unlock_duration_seconds_bucket`, `type="btree-get-lock"`),
				c.getMetricWithFilter(`mo_txn_unlock_duration_seconds_bucket`, `type="btree-total"`),
			},
			[]string{
				"lock-total",
				"lock-wait",
				"unlock-total",
				"unlock-btree-get-lock",
				"unlock-btree-total",
			},
			[]float64{0.50, 0.8, 0.90, 0.99},
			[]float32{3, 3, 3, 3},
			axis.Unit("s"),
			axis.Min(0))...,
	)
}

func (c *DashboardCreator) initTxnLockWaitersRow() dashboard.Option {
	return dashboard.Row(
		"Txn lock waiters",
		c.getMultiHistogram(
			[]string{
				c.getMetricWithFilter(`mo_txn_lock_waiters_total_bucket`, ``),
			},
			[]string{
				"waiters",
			},
			[]float64{0.50, 0.8, 0.90, 0.99},
			[]float32{3, 3, 3, 3})...,
	)
}

func (c *DashboardCreator) initTxnMpoolRow() dashboard.Option {
	return dashboard.Row(
		"Txn MPool",
		c.getMultiHistogram(
			[]string{
				c.getMetricWithFilter(`mo_txn_mpool_duration_seconds_bucket`, `type="new"`),
				c.getMetricWithFilter(`mo_txn_mpool_duration_seconds_bucket`, `type="alloc"`),
				c.getMetricWithFilter(`mo_txn_mpool_duration_seconds_bucket`, `type="free"`),
				c.getMetricWithFilter(`mo_txn_mpool_duration_seconds_bucket`, `type="delete"`),
			},
			[]string{
				"new",
				"alloc",
				"free",
				"delete",
			},
			[]float64{0.50, 0.8, 0.90, 0.99},
			[]float32{3, 3, 3, 3},
			axis.Unit("s"),
			axis.Min(0))...,
	)
}

func (c *DashboardCreator) initTxnUnlockTablesRow() dashboard.Option {
	return dashboard.Row(
		"Txn unlock tables",
		c.getMultiHistogram(
			[]string{
				c.getMetricWithFilter(`mo_txn_unlock_table_total_bucket`, ``),
			},
			[]string{
				"tables",
			},
			[]float64{0.50, 0.8, 0.90, 0.99},
			[]float32{3, 3, 3, 3})...,
	)
}

func (c *DashboardCreator) initTxnReaderDurationRow() dashboard.Option {
	return dashboard.Row(
		"Txn reader duration",
		c.getMultiHistogram(
			[]string{
				c.getMetricWithFilter(`mo_txn_reader_duration_seconds_bucket`, `type="block-reader"`),
				c.getMetricWithFilter(`mo_txn_reader_duration_seconds_bucket`, `type="merge-reader"`),
				c.getMetricWithFilter(`mo_txn_reader_duration_seconds_bucket`, `type="block-merge-reader"`),
			},
			[]string{
				"block-reader",
				"merge-reader",
				"block-merge-reader",
			},
			[]float64{0.80, 0.90, 0.95, 0.99},
			[]float32{3, 3, 3, 3},
			axis.Unit("s"),
			axis.Min(0))...,
	)
}
