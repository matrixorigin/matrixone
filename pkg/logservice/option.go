// Copyright 2021 - 2022 Matrix Origin
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

package logservice

import (
	"context"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/util"

	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
)

// Option is utility that sets callback for Service.
type Option func(*Service)

// WithBackendFilter sets filter via which could select remote backend.
func WithBackendFilter(filter func(morpc.Message, string) bool) Option {
	return func(s *Service) {
		s.options.backendFilter = filter
	}
}

// WithRuntime sets runtime
func WithRuntime(runtime runtime.Runtime) Option {
	return func(s *Service) {
		s.runtime = runtime
	}
}

// WithTaskStorageFactory set up the special task storage factory
func WithTaskStorageFactory(factory taskservice.TaskStorageFactory) Option {
	return func(s *Service) {
		s.task.storageFactory = factory
	}
}

// WithDataSync sets the DataSync of service.
func WithDataSync(ds DataSync) Option {
	return func(s *Service) {
		s.dataSync = ds
	}
}

// WithConfigData saves the data from the config file
func WithConfigData(data map[string]*pb.ConfigItem) Option {
	return func(s *Service) {
		if s.config == nil {
			s.config = util.NewConfigData(data)
		} else {
			util.MergeConfig(s.config, data)
		}
	}
}

type ContextKey string

const (
	BackendOption ContextKey = "morpc.BackendOption"
	ClientOption  ContextKey = "morpc.ClientOption"
)

func GetBackendOptions(ctx context.Context) []morpc.BackendOption {
	if v := ctx.Value(BackendOption); v != nil {
		return v.([]morpc.BackendOption)
	}
	return nil
}

func GetClientOptions(ctx context.Context) []morpc.ClientOption {
	if v := ctx.Value(ClientOption); v != nil {
		return v.([]morpc.ClientOption)
	}
	return nil
}

func SetBackendOptions(ctx context.Context, opts ...morpc.BackendOption) context.Context {
	return context.WithValue(ctx, BackendOption, opts)
}

func SetClientOptions(ctx context.Context, opts ...morpc.ClientOption) context.Context {
	return context.WithValue(ctx, ClientOption, opts)
}
