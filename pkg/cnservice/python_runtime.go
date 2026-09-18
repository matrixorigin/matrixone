// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package cnservice

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/udf"
	"github.com/matrixorigin/matrixone/pkg/udf/python"
)

// buildCNRuntime builds the language registry used by the CN. A Python
// initialization failure is kept as a language-scoped unavailable runtime so
// the feature gate remains fail-closed without taking ordinary SQL down with
// it. Config validation still happens before NewService and remains fatal for
// malformed CN configuration.
func buildCNRuntime(cfg python.ClientConfig, fileService fileservice.FileService, logger *zap.Logger) (udf.Runtime, error) {
	if !cfg.Enabled {
		return udf.NewRuntime()
	}

	artifactStore, err := python.NewFileArtifactStore(fileService, python.DefaultMaxArtifactBytes)
	if err != nil {
		logPythonRuntimeDegraded(logger, err)
		return udf.NewRuntime(&unavailablePythonRuntime{cause: err})
	}
	runtime, err := python.NewGatewayWithArtifactStore(cfg, artifactStore)
	if err != nil {
		logPythonRuntimeDegraded(logger, err)
		return udf.NewRuntime(&unavailablePythonRuntime{cause: err})
	}
	return udf.NewRuntime(runtime)
}

func logPythonRuntimeDegraded(logger *zap.Logger, err error) {
	if logger != nil {
		logger.Warn("python udf runtime is unavailable; ordinary SQL remains enabled", zap.Error(err))
	}
}

// unavailablePythonRuntime preserves a stable language entry in the registry
// after initialization failed. It prevents a caller from mistaking an
// enabled-but-broken feature for a legacy fallback: every Python operation is
// rejected before user code, while StatusSnapshot exposes UNAVAILABLE.
type unavailablePythonRuntime struct {
	cause error
}

var _ udf.Runtime = (*unavailablePythonRuntime)(nil)
var _ udf.RuntimeReadiness = (*unavailablePythonRuntime)(nil)
var _ udf.RuntimeDefinitionValidator = (*unavailablePythonRuntime)(nil)
var _ udf.RuntimeStatusProvider = (*unavailablePythonRuntime)(nil)

func (r *unavailablePythonRuntime) Language() string { return udf.LanguagePython }

func (r *unavailablePythonRuntime) unavailableError() error {
	if r == nil || r.cause == nil {
		return fmt.Errorf("RESOURCE_UNAVAILABLE: Python UDF runtime initialization failed")
	}
	return fmt.Errorf("RESOURCE_UNAVAILABLE: Python UDF runtime initialization failed: %w", r.cause)
}

func (r *unavailablePythonRuntime) Execute(context.Context, *udf.Invocation, vector.FunctionResultWrapper, *mpool.MPool) error {
	return r.unavailableError()
}

func (r *unavailablePythonRuntime) CheckLanguageReady(ctx context.Context, language string) error {
	if language != udf.LanguagePython {
		return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: unsupported readiness language %q", language)
	}
	if ctx != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
	return r.unavailableError()
}

func (r *unavailablePythonRuntime) ValidateDefinition(ctx context.Context, definition *udf.RoutineDefinition) error {
	if definition == nil || definition.Language != udf.LanguagePython {
		return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: invalid Python definition")
	}
	return r.CheckLanguageReady(ctx, udf.LanguagePython)
}

func (r *unavailablePythonRuntime) StatusSnapshot(context.Context, string) udf.RuntimeStatusSnapshot {
	return udf.RuntimeStatusSnapshot{
		Language:   udf.LanguagePython,
		Enabled:    true,
		Ready:      false,
		ErrorClass: udf.RuntimeStatusUnavailable,
		Reason:     udf.RuntimeStatusReasonWorkerUnavailable,
	}
}
