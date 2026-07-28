// Copyright 2025 Matrix Origin
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

package fileservice

import (
	"context"
	"strings"
)

// ParallelMode controls when multipart parallel uploads are used.
type ParallelMode uint8

const (
	ParallelOff ParallelMode = iota
	ParallelAuto
	ParallelForce
)

func parseParallelMode(s string) (ParallelMode, bool) {
	switch strings.ToLower(s) {
	case "off", "false", "0", "":
		return ParallelOff, true
	case "auto":
		return ParallelAuto, true
	case "force", "on", "true", "1":
		return ParallelForce, true
	default:
		return ParallelOff, false
	}
}

type parallelModeKey struct{}

// WithParallelMode sets a per-call parallel mode override on context.
func WithParallelMode(ctx context.Context, mode ParallelMode) context.Context {
	return context.WithValue(ctx, parallelModeKey{}, mode)
}

// parallelModeFromContext retrieves a parallel mode override if present.
func parallelModeFromContext(ctx context.Context) (ParallelMode, bool) {
	if ctx == nil {
		return ParallelOff, false
	}
	if v := ctx.Value(parallelModeKey{}); v != nil {
		if mode, ok := v.(ParallelMode); ok {
			return mode, true
		}
	}
	return ParallelOff, false
}
