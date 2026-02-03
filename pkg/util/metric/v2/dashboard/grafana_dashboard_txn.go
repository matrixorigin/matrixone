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
	"fmt"

	"github.com/K-Phoen/grabana/axis"
	"github.com/K-Phoen/grabana/dashboard"
	"github.com/K-Phoen/grabana/row"
	"github.com/K-Phoen/grabana/timeseries"
)

func (c *DashboardCreator) initTxnDashboard() error {
	folder, err := c.createFolder(c.folderName)
	if err != nil {
		return err
	}

	build, err := dashboard.New(
		"Transaction Metrics",
		c.withRowOptions(
			// Overview and high-level metrics
			c.initTxnOverviewRow(),
			// Transaction lifecycle: Creation -> Execution -> Commit
			c.initTxnDurationRow(),
			c.initTxnStatementDurationRow(),
			c.initTxnStatementsCountRow(),
			c.initTxnCommitDurationRow(),
			c.initTxnTNSideRow(),
			// Lock operations
			c.initTxnLockDurationRow(),
			// Data access: Range scan, Read selectivity, Reader, Read Size
			c.initTxnTableRangesRow(),
			c.initTxnReadSelectivityRow(),
			c.initTxnReaderDurationRow(),
			c.initTxnReadSizeRow(),
			// Primary key operations
			c.initTxnPKMayBeChangedRow(),
			// Tombstone operations
			c.initTxnTombstoneRow(),
			// StarCount (SELECT COUNT(*) optimization)
			c.initTxnStarcountRow(),
			// Resource management
			c.initTxnMpoolRow(),
			c.initTxnExtraWorkspaceQuota(),
			// Special operations
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
		"Committed Object Locations (CN to TN)",
		c.withGraph(
			"Meta Location",
			6,
			`sum(`+c.getMetricWithFilter("mo_txn_cn_committed_location_quantity_size", `type="meta_location"`)+`)`,
			""),

		c.withGraph(
			"Delta Location",
			6,
			`sum(`+c.getMetricWithFilter("mo_txn_cn_committed_location_quantity_size", `type="delta_location"`)+`)`,
			""),
	)
}

func (c *DashboardCreator) initTxnTableRangesRow() dashboard.Option {
	durationHistogram := c.getHistogram(
		"Table Range Scan Duration",
		c.getMetricWithFilter(`mo_txn_ranges_duration_seconds_bucket`, ``),
		[]float64{0.50, 0.8, 0.90, 0.99},
		4,
		axis.Unit("s"),
		axis.Min(0))

	selectivityHistograms := c.getMultiHistogram(
		[]string{
			c.getMetricWithFilter(`mo_txn_ranges_selectivity_percentage_bucket`, `type="slow_path_block_selectivity"`),
			c.getMetricWithFilter(`mo_txn_ranges_selectivity_percentage_bucket`, `type="fast_path_block_selectivity"`),
			c.getMetricWithFilter(`mo_txn_ranges_selectivity_percentage_bucket`, `type="fast_path_obj_sort_key_zm_selectivity"`),
			c.getMetricWithFilter(`mo_txn_ranges_selectivity_percentage_bucket`, `type="fast_path_obj_column_zm_selectivity"`),
			c.getMetricWithFilter(`mo_txn_ranges_selectivity_percentage_bucket`, `type="fast_path_blk_column_zm_selectivity"`),
		},
		[]string{
			"Slow Path (Block)",
			"Fast Path (Block)",
			"Fast Path (Object Sort Key Zone Map)",
			"Fast Path (Object Column Zone Map)",
			"Fast Path (Block Column Zone Map)",
		},
		[]float64{0.50, 0.8, 0.90, 0.99},
		[]float32{3, 3, 3, 3},
		axis.Min(0))

	countHistograms := c.getMultiHistogram(
		[]string{
			c.getMetricWithFilter(`mo_txn_ranges_selected_block_cnt_total_bucket`, `type="slow_path_selected_block_cnt"`),
			c.getMetricWithFilter(`mo_txn_ranges_selected_block_cnt_total_bucket`, `type="fast_path_selected_block_cnt"`),
			c.getMetricWithFilter(`mo_txn_ranges_selected_block_cnt_total_bucket`, `type="fast_path_load_obj_cnt"`),
			c.getMetricWithFilter(`mo_txn_ranges_selected_block_cnt_total_bucket`, `type="slow_path_load_obj_cnt"`),
		},
		[]string{
			"Slow Path Selected Blocks",
			"Fast Path Selected Blocks",
			"Fast Path Loaded Objects",
			"Slow Path Loaded Objects",
		},
		[]float64{0.50, 0.8, 0.90, 0.99},
		[]float32{3, 3, 3, 3},
		axis.Min(0))

	options := []row.Option{durationHistogram}
	options = append(options, selectivityHistograms...)
	options = append(options, countHistograms...)

	return dashboard.Row(
		"Table Range Scan",
		options...,
	)
}

func (c *DashboardCreator) initTxnTombstoneRow() dashboard.Option {
	statisticsHistograms := c.getMultiHistogram(
		[]string{
			c.getMetricWithFilter(`mo_txn_reader_scanned_total_tombstone_bucket`, ``),
			c.getMetricWithFilter(`mo_txn_reader_each_blk_loaded_bucket`, ``),
			c.getMetricWithFilter(`mo_txn_reader_tombstone_selectivity_bucket`, `type="zm_selectivity"`),
			c.getMetricWithFilter(`mo_txn_reader_tombstone_selectivity_bucket`, `type="bl_selectivity"`),
		},
		[]string{
			"Total Scanned per Read",
			"Total Loaded per Read",
			"Zone Map Selectivity (Object)",
			"Block List Selectivity (Block)",
		},
		[]float64{0.50, 0.8, 0.90, 0.99},
		[]float32{3, 3, 3, 3},
		axis.Min(0))

	transferTimeSeries := c.getTimeSeries(
		"Transferred Tombstones Count",
		[]string{fmt.Sprintf(
			"sum (increase(%s[$interval]))",
			c.getMetricWithFilter(`mo_txn_transfer_tombstones_count_sum`, ""),
		)},
		[]string{"Count"},
		timeseries.Span(3))

	transferHistograms := []row.Option{
		c.getPercentHist(
			"Tombstone Transfer Duration",
			c.getMetricWithFilter(`mo_txn_transfer_duration_bucket`, `type="tombstones"`),
			[]float64{0.50, 0.8, 0.90, 0.99},
			SpanNulls(true),
			timeseries.Span(3)),
		c.getPercentHist(
			"Batch Tombstone Transfer Duration",
			c.getMetricWithFilter(`mo_txn_transfer_duration_bucket`, `type="batch"`),
			[]float64{0.50, 0.8, 0.90, 0.99},
			SpanNulls(true),
			timeseries.Span(3)),
	}

	s3CounterGraph := c.withMultiGraph(
		"S3 Tombstone Operations",
		3,
		[]string{
			`sum(rate(` + c.getMetricWithFilter("mo_txn_S3_tombstone", `type="softdelete objects"`) + `[$interval]))`,
			`sum(rate(` + c.getMetricWithFilter("mo_txn_S3_tombstone", `type="transfer data objects"`) + `[$interval]))`,
			`sum(rate(` + c.getMetricWithFilter("mo_txn_S3_tombstone", `type="transfer tombstones"`) + `[$interval]))`,
		},
		[]string{
			"Soft Delete Objects",
			"Transfer Data Objects",
			"Transfer Tombstones",
		})

	s3DurationHistograms := c.getMultiHistogram(
		[]string{
			c.getMetricWithFilter(`mo_txn_S3_tombstone_duration_bucket`, `step="1-GetSoftdeleteObjects"`),
			c.getMetricWithFilter(`mo_txn_S3_tombstone_duration_bucket`, `step="2-FindTombstonesOfObject"`),
			c.getMetricWithFilter(`mo_txn_S3_tombstone_duration_bucket`, `step="3-ReadTombstone"`),
			c.getMetricWithFilter(`mo_txn_S3_tombstone_duration_bucket`, `step="4-TransferDeleteRows"`),
		},
		[]string{
			"Get Soft Delete Objects",
			"Find Tombstones of Object",
			"Read Tombstone",
			"Transfer Delete Rows",
		},
		[]float64{0.50, 0.8, 0.90, 0.99},
		[]float32{3, 3, 3, 3},
		axis.Unit("s"),
		axis.Min(0))

	options := append([]row.Option{}, statisticsHistograms...)
	options = append(options, transferTimeSeries)
	options = append(options, transferHistograms...)
	options = append(options, s3CounterGraph)
	options = append(options, s3DurationHistograms...)

	return dashboard.Row(
		"Tombstone Operations",
		options...,
	)
}

func (c *DashboardCreator) initTxnStarcountRow() dashboard.Option {
	pathRate := c.getTimeSeries(
		"StarCount Path Rate",
		[]string{
			fmt.Sprintf(`sum(rate(%s[$interval])) by (path)`, c.getMetricWithFilter("mo_txn_starcount_path_total", "")),
		},
		[]string{"path"},
		timeseries.Span(4))

	durationHistogram := c.getHistogram(
		"StarCount Duration (总耗时)",
		c.getMetricWithFilter(`mo_txn_starcount_duration_seconds_bucket`, ``),
		[]float64{0.50, 0.8, 0.90, 0.99},
		6,
		axis.Unit("s"),
		axis.Min(0))

	resultRowsHistogram := c.getHistogram(
		"StarCount Result Rows (总行数)",
		c.getMetricWithFilter(`mo_txn_starcount_result_rows_bucket`, ``),
		[]float64{0.50, 0.8, 0.90, 0.99},
		6,
		axis.Min(0))

	estimateHistograms := c.getMultiHistogram(
		[]string{
			c.getMetricWithFilter(`mo_txn_starcount_estimate_tombstone_rows_bucket`, ``),
			c.getMetricWithFilter(`mo_txn_starcount_estimate_tombstone_objects_bucket`, ``),
		},
		[]string{
			"Estimated Tombstone Rows (Tombstone总行数, upper bound)",
			"Estimated Tombstone Objects (Tombstone objects 数量)",
		},
		[]float64{0.50, 0.8, 0.90, 0.99},
		[]float32{6, 6},
		axis.Min(0))

	// High ratio = estimate much larger than actual (e.g. 100x) = bad signal
	ratioHistogram := c.getHistogram(
		"Estimate / Actual Ratio (estimate 远大于 actual 时比值大)",
		c.getMetricWithFilter(`mo_txn_starcount_estimate_over_actual_ratio_bucket`, ``),
		[]float64{0.50, 0.8, 0.90, 0.99},
		6,
		axis.Min(1))

	options := []row.Option{pathRate, durationHistogram, resultRowsHistogram, ratioHistogram}
	options = append(options, estimateHistograms...)

	return dashboard.Row(
		"StarCount (SELECT COUNT(*) Optimization)",
		options...,
	)
}

func (c *DashboardCreator) initTxnReadSelectivityRow() dashboard.Option {
	// Block filter operations rate: number of blocks with filter operations per second
	// This metric records each block read that performs a filter operation, not high-level filter requests
	readFilterRequestRate := c.getTimeSeries(
		"Block filter operations rate",
		[]string{
			fmt.Sprintf(
				"sum(rate(%s[$interval])) by (%s)",
				c.getMetricWithFilter(`mo_txn_selectivity_count`, `type="readfilter_total"`), c.by),
		},
		[]string{fmt.Sprintf("blocks/sec-{{ %s }}", c.by)},
		timeseries.Span(4),
	)

	// Read filter completely filtered rate: rate of operations where all rows were filtered (len(sels) == 0)
	// Filtered_count / Total_count gives the ratio of completely filtered operations
	readFilterCompletelyFilteredRate := c.getTimeSeries(
		"Read filter completely filtered rate",
		[]string{
			fmt.Sprintf(
				`sum(rate(%s[$interval])) by (%s) / on(%s) sum(rate(%s[$interval])) by (%s)`,
				c.getMetricWithFilter(`mo_txn_selectivity_count`, `type="readfilter_filtered"`), c.by, c.by,
				c.getMetricWithFilter(`mo_txn_selectivity_count`, `type="readfilter_total"`), c.by),
		},
		[]string{fmt.Sprintf("completely-filtered-rate-{{ %s }}", c.by)},
		timeseries.Span(4),
	)

	// Column read histogram: show per-read column count percentiles
	// P50, P90, P99 of column read count per read operation
	columnReadCountHistogram := c.getHistogram(
		"Column read count per operation",
		c.getMetricWithFilter(`mo_txn_column_read_count_bucket`, `type="read"`),
		[]float64{0.50, 0.90, 0.99},
		6,
		axis.Min(0))

	// Column total histogram: show per-read total column count percentiles
	// P50, P90, P99 of total column count per read operation
	columnTotalCountHistogram := c.getHistogram(
		"Column total count per operation",
		c.getMetricWithFilter(`mo_txn_column_read_count_bucket`, `type="total"`),
		[]float64{0.50, 0.90, 0.99},
		6,
		axis.Min(0))

	// Column read ratio histogram: show ratio (read/total) percentiles
	// P50, P90, P99 of column read ratio per operation
	columnReadRatioHistogram := c.getTimeSeries(
		"Column read ratio per operation",
		[]string{
			fmt.Sprintf(
				"histogram_quantile(0.50, sum(rate(%s[$interval])) by (le)) / histogram_quantile(0.50, sum(rate(%s[$interval])) by (le))",
				c.getMetricWithFilter(`mo_txn_column_read_count_bucket`, `type="read"`),
				c.getMetricWithFilter(`mo_txn_column_read_count_bucket`, `type="total"`)),
			fmt.Sprintf(
				"histogram_quantile(0.90, sum(rate(%s[$interval])) by (le)) / histogram_quantile(0.90, sum(rate(%s[$interval])) by (le))",
				c.getMetricWithFilter(`mo_txn_column_read_count_bucket`, `type="read"`),
				c.getMetricWithFilter(`mo_txn_column_read_count_bucket`, `type="total"`)),
			fmt.Sprintf(
				"histogram_quantile(0.99, sum(rate(%s[$interval])) by (le)) / histogram_quantile(0.99, sum(rate(%s[$interval])) by (le))",
				c.getMetricWithFilter(`mo_txn_column_read_count_bucket`, `type="read"`),
				c.getMetricWithFilter(`mo_txn_column_read_count_bucket`, `type="total"`)),
		},
		[]string{"P50", "P90", "P99"},
		timeseries.Span(6),
	)

	return dashboard.Row(
		"Read Selectivity",
		readFilterRequestRate,
		readFilterCompletelyFilteredRate,
		columnReadCountHistogram,
		columnTotalCountHistogram,
		columnReadRatioHistogram,
	)
}

func (c *DashboardCreator) initTxnPKMayBeChangedRow() dashboard.Option {
	histograms := c.getMultiHistogram(
		[]string{
			c.getMetricWithFilter(`mo_txn_lazy_load_ckp_duration_seconds_bucket`, ``),
			c.getMetricWithFilter(`mo_txn_pk_exist_in_mem_duration_seconds_bucket`, ``),
		},
		[]string{
			"Lazy Load Checkpoint",
			"Primary Key Exist in Memory",
		},
		[]float64{0.50, 0.8, 0.90, 0.99},
		[]float32{3, 3, 3, 3},
		axis.Unit("s"),
		axis.Min(0))

	options := []row.Option{
		c.getHistogram(
			"Primary Key Duplicate Check Duration",
			c.getMetricWithFilter(`mo_txn_check_pk_dup_duration_seconds_bucket`, ``),
			[]float64{0.50, 0.8, 0.90, 0.99},
			6,
			axis.Unit("s"),
			axis.Min(0)),
		c.getHistogram(
			"Primary Key Change Detection Duration",
			c.getMetricWithFilter(`mo_txn_pk_may_be_changed_duration_seconds_bucket`, ``),
			[]float64{0.50, 0.8, 0.90, 0.99},
			6,
			axis.Unit("s"),
			axis.Min(0)),
	}
	options = append(options, histograms...)
	options = append(options,
		c.withMultiGraph(
			"Primary Key Change Detection Paths",
			4,
			[]string{
				`sum(rate(` + c.getMetricWithFilter("mo_txn_pk_may_be_changed_total", `type="total"`) + `[$interval]))`,
				`sum(rate(` + c.getMetricWithFilter("mo_txn_pk_may_be_changed_total", `type="mem_hit"`) + `[$interval]))`,
				`sum(rate(` + c.getMetricWithFilter("mo_txn_pk_may_be_changed_total", `type="mem_not_flushed"`) + `[$interval]))`,
				`sum(rate(` + c.getMetricWithFilter("mo_txn_pk_may_be_changed_total", `type="persisted"`) + `[$interval]))`,
			},
			[]string{
				"Total Checks",
				"Memory Hit",
				"Memory Not Flushed",
				"Persisted Storage",
			}),
		c.withMultiGraph(
			"Persisted Primary Key Change Check",
			3,
			[]string{
				`sum(rate(` + c.getMetricWithFilter("mo_txn_pk_change_check_total", `type="total"`) + `[$interval]))`,
				`sum(rate(` + c.getMetricWithFilter("mo_txn_pk_change_check_total", `type="changed"`) + `[$interval]))`,
				`sum(rate(` + c.getMetricWithFilter("mo_txn_pk_change_check_total", `type="io"`) + `[$interval]))`,
			},
			[]string{
				"Total Checks",
				"Changed Keys",
				"I/O Operations",
			}))

	return dashboard.Row(
		"Primary Key Operations",
		options...,
	)
}

func (c *DashboardCreator) initTxnOverviewRow() dashboard.Option {
	return dashboard.Row(
		"Transaction Overview",
		c.withMultiGraph(
			"Transaction Requests",
			3,
			[]string{
				`sum(rate(` + c.getMetricWithFilter("mo_txn_total", `type="user"`) + `[$interval]))`,
				`sum(rate(` + c.getMetricWithFilter("mo_txn_total", `type="internal"`) + `[$interval]))`,
				`sum(rate(` + c.getMetricWithFilter("mo_txn_total", `type="leak"`) + `[$interval]))`,
				`sum(rate(` + c.getMetricWithFilter("mo_txn_total", `type="long-running"`) + `[$interval]))`,
				`sum(rate(` + c.getMetricWithFilter("mo_txn_total", `type="stuck-in-commit"`) + `[$interval]))`,
				`sum(rate(` + c.getMetricWithFilter("mo_txn_total", `type="stuck-in-rollback"`) + `[$interval]))`,
				`sum(rate(` + c.getMetricWithFilter("mo_txn_statement_total", `type="total"`) + `[$interval]))`,
				`sum(rate(` + c.getMetricWithFilter("mo_txn_statement_total", `type="retry"`) + `[$interval]))`,
				`sum(rate(` + c.getMetricWithFilter("mo_txn_lock_total", `type="total"`) + `[$interval]))`,
				`sum(rate(` + c.getMetricWithFilter("mo_txn_lock_total", `type="local"`) + `[$interval]))`,
				`sum(rate(` + c.getMetricWithFilter("mo_txn_lock_total", `type="remote"`) + `[$interval]))`,
			},
			[]string{
				"User Transaction",
				"Internal Transaction",
				"Leaked Transaction",
				"Long Running Transaction",
				"Stuck in Commit",
				"Stuck in Rollback",
				"Statement",
				"Statement Retry",
				"Lock Operations",
				"Local Lock",
				"Remote Lock",
			}),

		c.withMultiGraph(
			"Commit Operations",
			3,
			[]string{
				`sum(rate(` + c.getMetricWithFilter("mo_txn_commit_total", `type="cn"`) + `[$interval]))`,
				`sum(rate(` + c.getMetricWithFilter("mo_txn_commit_total", `type="tn-receive"`) + `[$interval]))`,
				`sum(rate(` + c.getMetricWithFilter("mo_txn_commit_total", `type="tn-handle"`) + `[$interval]))`,
			},
			[]string{
				"CN Side",
				"TN Receive",
				"TN Handle",
			}),

		c.withMultiGraph(
			"Rollback Operations",
			3,
			[]string{
				`sum(rate(` + c.getMetricWithFilter("mo_txn_rollback_total", "") + `[$interval])) by (` + c.by + `)`,
				`sum(rate(` + c.getMetricWithFilter("mo_txn_rollback_last_statement_total", "") + `[$interval])) by (` + c.by + `)`,
				`sum(rate(` + c.getMetricWithFilter("mo_txn_user_rollback_total", "") + `[$interval])) by (` + c.by + `)`,
			},
			[]string{
				"Total Rollback",
				"Rollback Last Statement",
				"User Rollback",
			}),

		c.withMultiGraph(
			"Transaction Queue Status",
			3,
			[]string{
				`sum(` + c.getMetricWithFilter("mo_txn_queue_size", `type="active"`) + `)`,
				`sum(` + c.getMetricWithFilter("mo_txn_queue_size", `type="wait-active"`) + `)`,
				`sum(` + c.getMetricWithFilter("mo_txn_queue_size", `type="commit"`) + `)`,
				`sum(` + c.getMetricWithFilter("mo_txn_queue_size", `type="lock-rpc"`) + `)`,
			},
			[]string{
				"Active Queue",
				"Wait Active Queue",
				"Commit Queue",
				"Lock RPC Queue",
			}),
	)
}

func (c *DashboardCreator) initTxnDurationRow() dashboard.Option {
	return dashboard.Row(
		"Transaction Creation",
		c.getMultiHistogram(
			[]string{
				c.getMetricWithFilter(`mo_txn_life_duration_seconds_bucket`, ``),
				c.getMetricWithFilter(`mo_txn_create_duration_seconds_bucket`, `type="total"`),
				c.getMetricWithFilter(`mo_txn_create_duration_seconds_bucket`, `type="wait-active"`),
				c.getMetricWithFilter(`mo_txn_create_duration_seconds_bucket`, `type="determine-snapshot"`),
			},
			[]string{
				"Transaction Lifecycle",
				"Total Creation Time",
				"Wait for Active Transaction",
				"Determine Snapshot",
			},
			[]float64{0.50, 0.8, 0.90, 0.99},
			[]float32{3, 3, 3, 3},
			axis.Unit("s"),
			axis.Min(0))...,
	)
}

func (c *DashboardCreator) initTxnCommitDurationRow() dashboard.Option {
	return dashboard.Row(
		"Transaction Commit (CN Side)",
		c.getMultiHistogram(
			[]string{
				c.getMetricWithFilter("mo_txn_commit_duration_seconds_bucket", `type="cn"`),
				c.getMetricWithFilter(`mo_txn_commit_duration_seconds_bucket`, `type="cn-send"`),
				c.getMetricWithFilter(`mo_txn_commit_duration_seconds_bucket`, `type="cn-resp"`),
				c.getMetricWithFilter(`mo_txn_commit_duration_seconds_bucket`, `type="cn-wait-logtail"`),
				c.getMetricWithFilter(`mo_txn_commit_duration_seconds_bucket`, `type="tn"`),
			},
			[]string{
				"Total Commit Time",
				"Send Commit Request",
				"Receive Commit Response",
				"Wait for Logtail Applied",
				"TN Side Processing",
			},
			[]float64{0.50, 0.8, 0.90, 0.99},
			[]float32{3, 3, 3, 3},
			axis.Unit("s"),
			axis.Min(0))...,
	)
}

func (c *DashboardCreator) initTxnTNSideRow() dashboard.Option {
	prepareWALHistograms := c.getMultiHistogram(
		[]string{
			c.getMetricWithFilter("mo_txn_tn_side_duration_seconds_bucket", `step="1-PreparingWait"`),
			c.getMetricWithFilter("mo_txn_tn_side_duration_seconds_bucket", `step="2-Preparing"`),
			c.getMetricWithFilter("mo_txn_tn_side_duration_seconds_bucket", `step="3-PrepareWalWait"`),
			c.getMetricWithFilter("mo_txn_tn_side_duration_seconds_bucket", `step="4-PrepareWal"`),
			c.getMetricWithFilter("mo_txn_tn_side_duration_seconds_bucket", `step="5-PreparedWait"`),
			c.getMetricWithFilter("mo_txn_tn_side_duration_seconds_bucket", `step="6-Prepared"`),
		},
		[]string{
			"Preparing Wait",
			"Preparing",
			"Prepare WAL Wait",
			"Prepare WAL",
			"Prepared Wait",
			"Prepared",
		},
		[]float64{0.80, 0.90, 0.95, 0.99},
		[]float32{3, 3, 3, 3},
		axis.Unit("s"),
		axis.Min(0))

	deduplicateHistograms := c.getMultiHistogram(
		[]string{
			c.getMetricWithFilter("mo_txn_tn_deduplicate_duration_seconds_bucket", `type="append_deduplicate"`),
			c.getMetricWithFilter("mo_txn_tn_deduplicate_duration_seconds_bucket", `type="prePrepare_deduplicate"`),
		},
		[]string{
			"Append Deduplicate",
			"Pre-Prepare Deduplicate",
		},
		[]float64{0.80, 0.90, 0.95, 0.99},
		[]float32{3, 3, 3, 3},
		axis.Unit("s"),
		axis.Min(0))

	options := append([]row.Option{}, prepareWALHistograms...)
	options = append(options, deduplicateHistograms...)
	options = append(options,
		c.getHistogram(
			"LogService Append Duration",
			c.getMetricWithFilter("mo_txn_tn_logservice_append_duration_seconds_bucket", ``),
			[]float64{0.50, 0.90, 0.99},
			4,
			axis.Unit("s"),
			axis.Min(0)),
		c.getHistogram(
			"Handle Commit Before Transaction Commit",
			c.getMetricWithFilter("mo_txn_tn_side_duration_seconds_bucket", `step="before_txn_commit"`),
			[]float64{0.50, 0.8, 0.90, 0.99},
			4,
			axis.Unit("s"),
			axis.Min(0)))

	return dashboard.Row(
		"Transaction Processing (TN Side)",
		options...,
	)
}

func (c *DashboardCreator) initTxnStatementDurationRow() dashboard.Option {
	return dashboard.Row(
		"Statement Execution",
		c.getMultiHistogram(
			[]string{
				c.getMetricWithFilter(`mo_txn_statement_duration_seconds_bucket`, `type="execute"`),
				c.getMetricWithFilter(`mo_txn_statement_duration_seconds_bucket`, `type="execute-latency"`),
				c.getMetricWithFilter(`mo_txn_statement_duration_seconds_bucket`, `type="build-plan"`),
				c.getMetricWithFilter(`mo_txn_statement_duration_seconds_bucket`, `type="compile"`),
				c.getMetricWithFilter(`mo_txn_statement_duration_seconds_bucket`, `type="scan"`),
				c.getMetricWithFilter(`mo_txn_statement_duration_seconds_bucket`, `type="external-scan"`),
				c.getMetricWithFilter(`mo_txn_statement_duration_seconds_bucket`, `type="insert-s3"`),
				c.getMetricWithFilter(`mo_txn_statement_duration_seconds_bucket`, `type="stats"`),
				c.getMetricWithFilter(`mo_txn_statement_duration_seconds_bucket`, `type="resolve"`),
				c.getMetricWithFilter(`mo_txn_statement_duration_seconds_bucket`, `type="resolve-udf"`),
				c.getMetricWithFilter(`mo_txn_statement_duration_seconds_bucket`, `type="update-stats"`),
				c.getMetricWithFilter(`mo_txn_statement_duration_seconds_bucket`, `type="update-info-from-zonemap"`),
				c.getMetricWithFilter(`mo_txn_statement_duration_seconds_bucket`, `type="update-stats-info-map"`),
				c.getMetricWithFilter(`mo_txn_statement_duration_seconds_bucket`, `type="nodes"`),
				c.getMetricWithFilter(`mo_txn_statement_duration_seconds_bucket`, `type="compileScope"`),
				c.getMetricWithFilter(`mo_txn_statement_duration_seconds_bucket`, `type="compileQuery"`),
				c.getMetricWithFilter(`mo_txn_statement_duration_seconds_bucket`, `type="compilePlanScope"`),
				c.getMetricWithFilter(`mo_txn_statement_duration_seconds_bucket`, `type="BuildPlan"`),
				c.getMetricWithFilter(`mo_txn_statement_duration_seconds_bucket`, `type="BuildSelect"`),
				c.getMetricWithFilter(`mo_txn_statement_duration_seconds_bucket`, `type="BuildInsert"`),
				c.getMetricWithFilter(`mo_txn_statement_duration_seconds_bucket`, `type="BuildExplain"`),
				c.getMetricWithFilter(`mo_txn_statement_duration_seconds_bucket`, `type="BuildReplace"`),
				c.getMetricWithFilter(`mo_txn_statement_duration_seconds_bucket`, `type="BuildUpdate"`),
				c.getMetricWithFilter(`mo_txn_statement_duration_seconds_bucket`, `type="BuildDelete"`),
				c.getMetricWithFilter(`mo_txn_statement_duration_seconds_bucket`, `type="BuildLoad"`),
			},
			[]string{
				"Execute",
				"Execute Latency",
				"Build Plan",
				"Compile",
				"Scan",
				"External Scan",
				"Insert to S3",
				"Statistics",
				"Resolve",
				"Resolve UDF",
				"Update Statistics",
				"Update Info from Zone Map",
				"Update Stats Info Map",
				"Nodes",
				"Compile Scope",
				"Compile Query",
				"Compile Plan Scope",
				"Build Plan",
				"Build Select",
				"Build Insert",
				"Build Explain",
				"Build Replace",
				"Build Update",
				"Build Delete",
				"Build Load",
			},
			[]float64{0.50, 0.8, 0.90, 0.99},
			[]float32{3, 3, 3, 3},
			axis.Unit("s"),
			axis.Min(0))...,
	)
}

func (c *DashboardCreator) initTxnStatementsCountRow() dashboard.Option {
	return dashboard.Row(
		"Statements per Transaction",
		c.getMultiHistogram(
			[]string{
				c.getMetricWithFilter(`mo_txn_life_statements_total_bucket`, ``),
			},
			[]string{
				"Statements per Transaction",
			},
			[]float64{0.50, 0.8, 0.90, 0.99},
			[]float32{3, 3, 3, 3})...,
	)
}

func (c *DashboardCreator) initTxnShowAccountsRow() dashboard.Option {
	return dashboard.Row(
		"Show Accounts",
		c.getHistogram(
			"Show Accounts Duration",
			c.getMetricWithFilter(`mo_txn_show_accounts_duration_seconds_bucket`, ``),
			[]float64{0.50, 0.8, 0.90, 0.99},
			12,
			axis.Unit("s"),
			axis.Min(0)),
	)
}

func (c *DashboardCreator) initTxnLockDurationRow() dashboard.Option {
	lockDurationHistograms := c.getMultiHistogram(
		[]string{
			c.getMetricWithFilter(`mo_txn_lock_duration_seconds_bucket`, `type="acquire"`),
			c.getMetricWithFilter(`mo_txn_lock_duration_seconds_bucket`, `type="acquire-wait"`),
			c.getMetricWithFilter(`mo_txn_unlock_duration_seconds_bucket`, `type="total"`),
			c.getMetricWithFilter(`mo_txn_unlock_duration_seconds_bucket`, `type="btree-get-lock"`),
			c.getMetricWithFilter(`mo_txn_unlock_duration_seconds_bucket`, `type="btree-total"`),
			c.getMetricWithFilter(`mo_txn_unlock_duration_seconds_bucket`, `type="worker-handle"`),
		},
		[]string{
			"Acquire Lock",
			"Acquire Lock Wait",
			"Unlock Total",
			"Unlock BTree Get Lock",
			"Unlock BTree Total",
			"Worker Handle",
		},
		[]float64{0.50, 0.8, 0.90, 0.99},
		[]float32{3, 3, 3, 3},
		axis.Unit("s"),
		axis.Min(0))

	lockStatsHistograms := c.getMultiHistogram(
		[]string{
			c.getMetricWithFilter(`mo_txn_lock_waiters_total_bucket`, ``),
			c.getMetricWithFilter(`mo_txn_unlock_table_total_bucket`, ``),
		},
		[]string{
			"Number of Waiters per Lock",
			"Number of Tables per Unlock",
		},
		[]float64{0.50, 0.8, 0.90, 0.99},
		[]float32{3, 3, 3, 3})

	options := append([]row.Option{}, lockDurationHistograms...)
	options = append(options, lockStatsHistograms...)

	return dashboard.Row(
		"Lock Operations",
		options...,
	)
}

func (c *DashboardCreator) initTxnMpoolRow() dashboard.Option {
	return dashboard.Row(
		"Memory Pool Operations",
		c.getMultiHistogram(
			[]string{
				c.getMetricWithFilter(`mo_txn_mpool_duration_seconds_bucket`, `type="new"`),
				c.getMetricWithFilter(`mo_txn_mpool_duration_seconds_bucket`, `type="delete"`),
			},
			[]string{
				"Allocate",
				"Deallocate",
			},
			[]float64{0.50, 0.8, 0.90, 0.99},
			[]float32{3, 3, 3, 3},
			axis.Unit("s"),
			axis.Min(0))...,
	)
}

func (c *DashboardCreator) initTxnReaderDurationRow() dashboard.Option {
	return dashboard.Row(
		"Data Reader",
		c.getMultiHistogram(
			[]string{
				c.getMetricWithFilter(`mo_txn_reader_duration_seconds_bucket`, `type="block-reader"`),
				c.getMetricWithFilter(`mo_txn_reader_duration_seconds_bucket`, `type="merge-reader"`),
				c.getMetricWithFilter(`mo_txn_reader_duration_seconds_bucket`, `type="block-merge-reader"`),
			},
			[]string{
				"Block Reader",
				"Merge Reader",
				"Block Merge Reader",
			},
			[]float64{0.80, 0.90, 0.95, 0.99},
			[]float32{3, 3, 3, 3},
			axis.Unit("s"),
			axis.Min(0))...,
	)
}

func (c *DashboardCreator) initTxnReadSizeRow() dashboard.Option {
	return dashboard.Row(
		"Read Size",
		c.getMultiHistogram(
			[]string{
				c.getMetricWithFilter(`mo_txn_read_size_bytes_bucket`, `type="total"`),
				c.getMetricWithFilter(`mo_txn_read_size_bytes_bucket`, `type="s3"`),
				c.getMetricWithFilter(`mo_txn_read_size_bytes_bucket`, `type="disk"`),
			},
			[]string{
				"Total Read Size",
				"S3 Read Size",
				"Disk Read Size",
			},
			[]float64{0.50, 0.80, 0.90, 0.99},
			[]float32{4, 4, 4, 4},
			axis.Unit("decbytes"),
			axis.Min(0))...,
	)
}

func (c *DashboardCreator) initTxnExtraWorkspaceQuota() dashboard.Option {
	return dashboard.Row(
		"Extra Workspace Quota",
		c.withGraph(
			"Extra Workspace Quota",
			12,
			`sum(`+c.getMetricWithFilter("mo_txn_extra_workspace_quota", ``)+`)`,
			"{{ "+c.by+" }}", axis.Unit("decbytes")),
	)
}
