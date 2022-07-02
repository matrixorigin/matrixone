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

package service

import (
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"go.uber.org/zap"
)

var _ TxnService = (*service)(nil)

type service struct {
	cfg    Config
	sender rpc.TxnSender // send txn requests to other txn service
}

func NewTxnService(cfg Config) (TxnService, error) {
	cfg.validate()

	sender, err := createSender(cfg)
	if err != nil {
		return nil, err
	}

	return newTxnServiceWithSender(cfg, sender)
}

func newTxnServiceWithSender(cfg Config, sender rpc.TxnSender) (TxnService, error) {
	cfg.validate()
	return &service{sender: sender}, nil
}

func (s *service) Metadata() metadata.DNShard {
	return s.cfg.Metadata
}

func (s *service) Close() error {
	return s.sender.Close()
}

func (s *service) validDNShard(request *txn.TxnRequest) {
	dn := request.GetTargetDN()

	if dn.ShardID != s.cfg.Metadata.ShardID ||
		dn.ReplicaID != s.cfg.Metadata.ReplicaID {
		s.cfg.Logger.Fatal("DN metadata not match",
			zap.String("request-dn", dn.DebugString()),
			zap.String("local-dn", s.cfg.Metadata.DebugString()))
	}
}

func createSender(cfg Config) (rpc.TxnSender, error) {
	return nil, nil
}
