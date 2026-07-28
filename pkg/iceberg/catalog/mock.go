// Copyright 2026 Matrix Origin
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

package catalog

import (
	"context"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	icebergio "github.com/matrixorigin/matrixone/pkg/iceberg/io"
)

type MockClient struct {
	Name string

	GetConfigFunc       func(context.Context, api.GetConfigRequest) (*api.ConfigResponse, error)
	ListNamespacesFunc  func(context.Context, api.ListNamespacesRequest) (*api.ListNamespacesResponse, error)
	ListTablesFunc      func(context.Context, api.ListTablesRequest) (*api.ListTablesResponse, error)
	LoadTableFunc       func(context.Context, api.LoadTableRequest) (*api.LoadTableResponse, error)
	LoadCredentialsFunc func(context.Context, api.LoadCredentialsRequest) (*api.LoadCredentialsResponse, error)
	CreateTableFunc     func(context.Context, api.CreateTableRequest) (*api.CreateTableResponse, error)
	CommitTableFunc     func(context.Context, api.CommitRequest) (*api.CommitResult, error)
	NewRemoteSignerFunc func(api.CatalogRequest, map[string]string) icebergio.RemoteSigner

	mu    sync.Mutex
	Calls []string
}

func (m *MockClient) AdapterName() string {
	if m.Name == "" {
		return "mock"
	}
	return m.Name
}

func (m *MockClient) GetConfig(ctx context.Context, req api.GetConfigRequest) (*api.ConfigResponse, error) {
	m.record("GetConfig")
	if m.GetConfigFunc != nil {
		return m.GetConfigFunc(ctx, req)
	}
	return &api.ConfigResponse{}, nil
}

func (m *MockClient) ListNamespaces(ctx context.Context, req api.ListNamespacesRequest) (*api.ListNamespacesResponse, error) {
	m.record("ListNamespaces")
	if m.ListNamespacesFunc != nil {
		return m.ListNamespacesFunc(ctx, req)
	}
	return &api.ListNamespacesResponse{}, nil
}

func (m *MockClient) ListTables(ctx context.Context, req api.ListTablesRequest) (*api.ListTablesResponse, error) {
	m.record("ListTables")
	if m.ListTablesFunc != nil {
		return m.ListTablesFunc(ctx, req)
	}
	return &api.ListTablesResponse{}, nil
}

func (m *MockClient) LoadTable(ctx context.Context, req api.LoadTableRequest) (*api.LoadTableResponse, error) {
	m.record("LoadTable")
	if m.LoadTableFunc != nil {
		return m.LoadTableFunc(ctx, req)
	}
	return &api.LoadTableResponse{}, nil
}

func (m *MockClient) LoadCredentials(ctx context.Context, req api.LoadCredentialsRequest) (*api.LoadCredentialsResponse, error) {
	m.record("LoadCredentials")
	if m.LoadCredentialsFunc != nil {
		return m.LoadCredentialsFunc(ctx, req)
	}
	return &api.LoadCredentialsResponse{}, nil
}

func (m *MockClient) CreateTable(ctx context.Context, req api.CreateTableRequest) (*api.CreateTableResponse, error) {
	m.record("CreateTable")
	if m.CreateTableFunc != nil {
		return m.CreateTableFunc(ctx, req)
	}
	return &api.CreateTableResponse{}, nil
}

func (m *MockClient) CommitTable(ctx context.Context, req api.CommitRequest) (*api.CommitResult, error) {
	m.record("CommitTable")
	if m.CommitTableFunc != nil {
		return m.CommitTableFunc(ctx, req)
	}
	return &api.CommitResult{}, nil
}

func (m *MockClient) NewRemoteSigner(req api.CatalogRequest, config map[string]string) icebergio.RemoteSigner {
	m.record("NewRemoteSigner")
	if m.NewRemoteSignerFunc != nil {
		return m.NewRemoteSignerFunc(req, config)
	}
	return nil
}

func (m *MockClient) record(call string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Calls = append(m.Calls, call)
}
