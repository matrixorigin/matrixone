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

package config

import (
	"context"
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
)

type CacheSizeType uint8

const (
	CST_None CacheSizeType = iota
	CST_Customize
)

func WithQuickScanAndCKPOpts2(in *options.Options, factor int) (opts *options.Options) {
	opts = WithQuickScanAndCKPOpts(in)
	opts.CheckpointCfg.ScanInterval *= time.Duration(factor)
	opts.CheckpointCfg.FlushInterval *= time.Duration(factor)
	opts.CheckpointCfg.MinCount = int64(factor)
	opts.CheckpointCfg.IncrementalInterval *= time.Duration(factor)
	opts.CheckpointCfg.BlockRows = 10
	opts.Ctx = context.Background()
	return opts
}

func WithQuickScanAndCKPOpts(in *options.Options) (opts *options.Options) {
	if in == nil {
		opts = new(options.Options)
	} else {
		opts = in
	}
	opts.CheckpointCfg = new(options.CheckpointCfg)
	opts.CheckpointCfg.ScanInterval = time.Millisecond * 10
	opts.CheckpointCfg.FlushInterval = time.Millisecond * 10
	opts.CheckpointCfg.MinCount = 1
	opts.CheckpointCfg.IncrementalInterval = time.Millisecond * 20
	opts.CheckpointCfg.GlobalMinCount = 1
	opts.CheckpointCfg.GCCheckpointInterval = time.Millisecond * 10
	opts.CheckpointCfg.BlockRows = 10
	opts.CheckpointCfg.GlobalVersionInterval = time.Millisecond * 10
	opts.GCCfg = new(options.GCCfg)
	opts.GCCfg.ScanGCInterval = time.Millisecond * 10
	opts.GCCfg.GCTTL = time.Millisecond * 1
	opts.GCCfg.CacheSize = 1
	opts.GCCfg.GCProbility = 0.000001
	opts.GCCfg.GCDeleteBatchSize = 2
	opts.CatalogCfg = new(options.CatalogCfg)
	opts.CatalogCfg.GCInterval = time.Millisecond * 1
	opts.Ctx = context.Background()
	return opts
}

func WithQuickScanAndCKPAndGCOpts(in *options.Options) (opts *options.Options) {
	if in == nil {
		opts = new(options.Options)
	} else {
		opts = in
	}
	opts.CheckpointCfg = new(options.CheckpointCfg)
	opts.CheckpointCfg.ScanInterval = time.Millisecond * 10
	opts.CheckpointCfg.FlushInterval = time.Millisecond * 10
	opts.CheckpointCfg.MinCount = 1
	opts.CheckpointCfg.IncrementalInterval = time.Millisecond * 20
	opts.CheckpointCfg.GlobalMinCount = 1
	opts.CheckpointCfg.BlockRows = 10

	opts.GCCfg = new(options.GCCfg)
	// ScanGCInterval does not need to be too fast, because manual gc will be performed in the case
	opts.GCCfg.ScanGCInterval = time.Second * 10
	opts.CatalogCfg = new(options.CatalogCfg)
	opts.CatalogCfg.GCInterval = time.Millisecond * 1
	opts.GCCfg.GCTTL = time.Millisecond * 1
	opts.GCCfg.GCDeleteBatchSize = 2
	opts.Ctx = context.Background()
	return opts
}

func WithOpts(in *options.Options, factor float64) (opts *options.Options) {
	if in == nil {
		opts = new(options.Options)
	} else {
		opts = in
	}
	opts.CheckpointCfg = new(options.CheckpointCfg)
	opts.CheckpointCfg.ScanInterval = time.Second * time.Duration(factor)
	opts.CheckpointCfg.FlushInterval = time.Second * time.Duration(factor)
	opts.CheckpointCfg.MinCount = 1 * int64(factor)
	opts.CheckpointCfg.IncrementalInterval = time.Second * 2 * time.Duration(factor)
	opts.CheckpointCfg.GlobalMinCount = 10
	opts.CheckpointCfg.BlockRows = 10
	opts.Ctx = context.Background()
	return opts
}

func WithLongScanAndCKPOpts(in *options.Options) (opts *options.Options) {
	if in == nil {
		opts = new(options.Options)
	} else {
		opts = in
	}
	opts.CheckpointCfg = new(options.CheckpointCfg)
	opts.CheckpointCfg.ScanInterval = time.Hour
	opts.CheckpointCfg.MinCount = 100000000
	opts.CheckpointCfg.IncrementalInterval = time.Hour
	opts.CheckpointCfg.GlobalMinCount = 10000000
	opts.CheckpointCfg.BlockRows = 10
	opts.Ctx = context.Background()
	return opts
}

func WithLongScanAndCKPOptsAndQuickGC(in *options.Options) (opts *options.Options) {
	if in == nil {
		opts = new(options.Options)
	} else {
		opts = in
	}
	opts.CheckpointCfg = new(options.CheckpointCfg)
	opts.CheckpointCfg.ScanInterval = time.Hour
	opts.CheckpointCfg.MinCount = 100000000
	opts.CheckpointCfg.IncrementalInterval = time.Hour
	opts.CheckpointCfg.GlobalMinCount = 10000000
	opts.CheckpointCfg.BlockRows = 10
	opts.GCCfg = new(options.GCCfg)
	opts.GCCfg.ScanGCInterval = time.Second * 10
	opts.GCCfg.GCTTL = time.Millisecond * 1
	opts.GCCfg.GCDeleteBatchSize = 2
	opts.Ctx = context.Background()
	return opts
}
