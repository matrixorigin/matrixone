// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package python

import (
	"context"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

type Config struct {
	UUID    string `toml:"uuid"`
	Address string `toml:"address"`
	Path    string `toml:"path"`
	Python  string `toml:"python"`
}

func (c *Config) Validate() error {
	if c.Address == "" {
		return moerr.NewInternalError(context.Background(), "missing python udf address")
	}
	if c.Path == "" {
		return moerr.NewInternalError(context.Background(), "missing python udf path")
	}
	return nil
}

type ClientConfig struct {
	// Enabled is deliberately false by default.  The Python worker is an
	// unisolated development adapter until the production sandbox contract is
	// deployed, so a generic CN launch must never attach it implicitly.
	Enabled         bool          `toml:"enabled"`
	AllowUnisolated bool          `toml:"allow-unisolated"`
	ServerAddress   string        `toml:"server-address"`
	MaxBatchBytes   int64         `toml:"max-batch-bytes"`
	MaxBatchRows    int64         `toml:"max-batch-rows"`
	RequestTimeout  time.Duration `toml:"request-timeout"`
}

func (c *ClientConfig) Validate() error {
	if !c.Enabled {
		return nil
	}
	if !c.AllowUnisolated {
		return moerr.NewInternalError(context.Background(), "python udf requires explicit allow-unisolated until the sandbox is configured")
	}
	if c.ServerAddress == "" {
		return moerr.NewInternalError(context.Background(), "missing python udf address")
	}
	if c.MaxBatchBytes < 0 || c.MaxBatchBytes > 1<<30 {
		return moerr.NewInternalError(context.Background(), "invalid python udf max batch bytes")
	}
	if c.MaxBatchRows < 0 || c.MaxBatchRows > 1<<30 {
		return moerr.NewInternalError(context.Background(), "invalid python udf max batch rows")
	}
	if c.RequestTimeout < 0 {
		return moerr.NewInternalError(context.Background(), "invalid python udf request timeout")
	}
	return nil
}
