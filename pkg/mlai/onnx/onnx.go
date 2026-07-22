// Copyright 2021 - 2025 Matrix Origin
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

// Package onnx isolates all use of the onnxruntime_go library (which dlopens
// the onnxruntime shared library at runtime). The onnx_run builtin SQL
// function is a thin wrapper over this package.
//
// The onnxruntime environment is initialized lazily and at most once. If the
// shared library cannot be loaded, initialization fails but is remembered:
// every later call returns that error, while the database itself starts
// normally (initialization is never triggered at process start).
package onnx

import (
	"os"
	"path/filepath"
	"runtime"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	ort "github.com/yalue/onnxruntime_go"
)

// EnvLibPath lets an operator point MatrixOne at a specific onnxruntime
// shared library, overriding the default search.
const EnvLibPath = "MO_ONNXRUNTIME_LIB"

var (
	initOnce sync.Once
	initErr  error
)

// sharedLibName returns the platform-specific onnxruntime library file name,
// matching what thirdparties/Makefile downloads.
func sharedLibName() string {
	switch runtime.GOOS {
	case "darwin":
		// Only arm64 is packaged for macOS.
		return "onnxruntime_arm64.dylib"
	case "linux":
		if runtime.GOARCH == "arm64" {
			return "onnxruntime_arm64.so"
		}
		return "onnxruntime.so"
	default:
		return ""
	}
}

// resolveLibPath finds the onnxruntime shared library. Search order:
//  1. $MO_ONNXRUNTIME_LIB (explicit path);
//  2. next to the mo-service binary (production layout);
//  3. thirdparties/install/lib/<name> discovered by walking up from the
//     working directory (dev / test layout).
func resolveLibPath() (string, error) {
	if p := os.Getenv(EnvLibPath); p != "" {
		if fileExists(p) {
			return p, nil
		}
		return "", moerr.NewInternalErrorNoCtxf(
			"onnx: %s points to a non-existent file: %s", EnvLibPath, p)
	}

	name := sharedLibName()
	if name == "" {
		return "", moerr.NewInternalErrorNoCtxf(
			"onnx: unsupported platform %s/%s", runtime.GOOS, runtime.GOARCH)
	}

	// (2) Beside the executable, or in its lib/ subdir (the mo-service rpath is
	// ${ORIGIN}/lib, so shared libs are shipped there).
	if exe, err := os.Executable(); err == nil {
		dir := filepath.Dir(exe)
		for _, cand := range []string{
			filepath.Join(dir, name),
			filepath.Join(dir, "lib", name),
		} {
			if fileExists(cand) {
				return cand, nil
			}
		}
	}

	// (3) thirdparties/install/lib walking up from cwd.
	if wd, err := os.Getwd(); err == nil {
		dir := wd
		for {
			cand := filepath.Join(dir, "thirdparties", "install", "lib", name)
			if fileExists(cand) {
				return cand, nil
			}
			parent := filepath.Dir(dir)
			if parent == dir {
				break
			}
			dir = parent
		}
	}

	return "", moerr.NewInternalErrorNoCtxf(
		"onnx: could not locate onnxruntime shared library %q; set %s",
		name, EnvLibPath)
}

func fileExists(p string) bool {
	st, err := os.Stat(p)
	return err == nil && !st.IsDir()
}

// ensureInit initializes the onnxruntime environment exactly once. The result
// (nil or error) is cached; repeated calls are cheap.
func ensureInit() error {
	initOnce.Do(func() {
		path, err := resolveLibPath()
		if err != nil {
			initErr = err
			return
		}
		ort.SetSharedLibraryPath(path)
		if err := ort.InitializeEnvironment(); err != nil {
			initErr = moerr.NewInternalErrorNoCtxf(
				"onnx: failed to initialize onnxruntime from %s: %v", path, err)
			return
		}
	})
	return initErr
}

// Available reports whether the onnxruntime environment is usable. It triggers
// lazy initialization on first call. onnx_run must call this before doing any
// work so that a failed runtime surfaces as a clean SQL error rather than a
// crash, and so the database can start without onnxruntime present.
func Available() error {
	return ensureInit()
}
