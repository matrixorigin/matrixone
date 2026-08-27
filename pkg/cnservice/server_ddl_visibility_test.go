// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package cnservice

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
)

type ddlVisibilityTestCluster struct {
	cnServices   []metadata.CNService
	refreshCalls int
}

func (c *ddlVisibilityTestCluster) GetCNService(
	selector clusterservice.Selector,
	apply func(metadata.CNService) bool,
) {
	c.forEachCN(selector, apply)
}

func (*ddlVisibilityTestCluster) GetTNService(
	clusterservice.Selector,
	func(metadata.TNService) bool,
) {
}

func (*ddlVisibilityTestCluster) GetAllTNServices() []metadata.TNService { return nil }

func (c *ddlVisibilityTestCluster) GetCNServiceWithoutWorkingState(
	selector clusterservice.Selector,
	apply func(metadata.CNService) bool,
) {
	c.forEachCN(selector, apply)
}

func (c *ddlVisibilityTestCluster) forEachCN(
	selector clusterservice.Selector,
	apply func(metadata.CNService) bool,
) {
	for _, cn := range c.cnServices {
		if selector.MatchCN(cn) && !apply(cn) {
			return
		}
	}
}

func (*ddlVisibilityTestCluster) ForceRefresh(bool) {}
func (c *ddlVisibilityTestCluster) Refresh(context.Context) error {
	c.refreshCalls++
	return nil
}
func (*ddlVisibilityTestCluster) Close() {}
func (*ddlVisibilityTestCluster) DebugUpdateCNLabel(string, map[string][]string) error {
	return nil
}
func (*ddlVisibilityTestCluster) DebugUpdateCNWorkState(string, int) error { return nil }
func (*ddlVisibilityTestCluster) RemoveCN(string)                          {}
func (*ddlVisibilityTestCluster) AddCN(metadata.CNService)                 {}
func (*ddlVisibilityTestCluster) UpdateCN(metadata.CNService)              {}

type ddlVisibilityTestQueryClient struct {
	serviceID string
	frontiers map[string]timestamp.Timestamp
	requests  []string
	methods   []query.CmdMethod
	releases  int
}

func (c *ddlVisibilityTestQueryClient) ServiceID() string { return c.serviceID }
func (c *ddlVisibilityTestQueryClient) SendMessage(
	_ context.Context,
	address string,
	req *query.Request,
) (*query.Response, error) {
	c.requests = append(c.requests, address)
	c.methods = append(c.methods, req.CmdMethod)
	return &query.Response{GetCommit: &query.GetCommitResponse{
		CurrentCommitTS: c.frontiers[address],
	}}, nil
}
func (*ddlVisibilityTestQueryClient) NewRequest(method query.CmdMethod) *query.Request {
	return &query.Request{CmdMethod: method}
}
func (c *ddlVisibilityTestQueryClient) Release(*query.Response) { c.releases++ }
func (*ddlVisibilityTestQueryClient) Close() error              { return nil }

func TestPrepareDDLVisibilityBarrier(t *testing.T) {
	const serviceID = "ddl-visibility-startup-test"
	moruntime.SetupServiceBasedRuntime(serviceID, moruntime.DefaultRuntime())
	targetTS := timestamp.Timestamp{PhysicalTime: 200, LogicalTime: 3}
	cluster := &ddlVisibilityTestCluster{cnServices: []metadata.CNService{
		{
			ServiceID: serviceID, QueryAddress: "self:6001",
			ViewMetadataAdmissionGeneration: 7, DDLVisibilityBarrierReady: true,
		},
		{
			ServiceID: "peer", QueryAddress: "peer:6001",
			ViewMetadataAdmissionGeneration: 9, DDLVisibilityBarrierReady: true,
		},
	}}
	queryClient := &ddlVisibilityTestQueryClient{
		serviceID: serviceID,
		frontiers: map[string]timestamp.Timestamp{
			"self:6001": {PhysicalTime: 100},
			"peer:6001": targetTS,
		},
	}
	ctrl := gomock.NewController(t)
	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	txnClient.EXPECT().WaitLogTailAppliedAt(gomock.Any(), targetTS).Return(targetTS.Next(), nil)
	txnClient.EXPECT().SyncLatestCommitTS(targetTS)

	s := &service{
		cfg:                             &Config{UUID: serviceID},
		moCluster:                       cluster,
		queryClient:                     queryClient,
		_txnClient:                      txnClient,
		viewMetadataAdmissionGeneration: 7,
	}
	require.NoError(t, s.prepareDDLVisibilityBarrier())
	require.True(t, s.ddlVisibilityBarrierReady.Load())
	require.Equal(t, 1, cluster.refreshCalls)
	require.Equal(t, []string{"self:6001", "peer:6001"}, queryClient.requests)
	require.Equal(t, []query.CmdMethod{query.CmdMethod_GetCommit, query.CmdMethod_GetCommit}, queryClient.methods)
	require.Equal(t, 2, queryClient.releases)
}

func TestPrepareDDLVisibilityBarrierRejectsMissingProductionDependencies(t *testing.T) {
	const serviceID = "ddl-visibility-missing-dependency-test"
	moruntime.SetupServiceBasedRuntime(serviceID, moruntime.DefaultRuntime())
	s := &service{
		cfg:                             &Config{UUID: serviceID},
		viewMetadataAdmissionGeneration: 1,
	}

	err := s.prepareDDLVisibilityBarrier()
	require.ErrorContains(t, err, "dependencies are unavailable")
	require.True(t, s.ddlVisibilityBarrierReady.Load())
}

func TestPrepareDDLVisibilityBarrierSkipsFrontierSyncDuringRollingUpgrade(t *testing.T) {
	const serviceID = "ddl-visibility-mixed-version-test"
	rt := moruntime.DefaultRuntime()
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion31)
	moruntime.SetupServiceBasedRuntime(serviceID, rt)

	s := &service{cfg: &Config{UUID: serviceID}}
	require.NoError(t, s.prepareDDLVisibilityBarrier())
	require.True(t, s.ddlVisibilityBarrierReady.Load())
}

func TestWaitForDDLVisibilityBarrierPublicationHonorsCancellation(t *testing.T) {
	cluster := &ddlVisibilityTestCluster{}
	s := &service{
		cfg:                             &Config{UUID: "unpublished-cn"},
		moCluster:                       cluster,
		viewMetadataAdmissionGeneration: 1,
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()

	err := s.waitForDDLVisibilityBarrierPublication(ctx, time.Second)
	require.Error(t, err)
	require.Equal(t, 1, cluster.refreshCalls)
}

func TestSyncStartupDDLVisibilityFrontierAllowsEmptyFrontier(t *testing.T) {
	const serviceID = "ddl-visibility-empty-frontier-test"
	cluster := &ddlVisibilityTestCluster{cnServices: []metadata.CNService{{
		ServiceID: serviceID, QueryAddress: "self:6001",
		DDLVisibilityBarrierReady: true,
	}}}
	queryClient := &ddlVisibilityTestQueryClient{
		serviceID: serviceID,
		frontiers: map[string]timestamp.Timestamp{"self:6001": {}},
	}
	s := &service{moCluster: cluster, queryClient: queryClient}

	require.NoError(t, s.syncStartupDDLVisibilityFrontier(context.Background()))
	require.Equal(t, []string{"self:6001"}, queryClient.requests)
	require.Equal(t, 1, queryClient.releases)
}
