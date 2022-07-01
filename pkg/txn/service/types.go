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
// See the License for the s

package service

import (
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
)

// TxnService is a transaction service that runs on the DNStore and is used to receive transaction requests
// from the CN. In the case of a 2 pc distributed transaction, it acts as a transaction coordinator to handle
// distributed transactions.
type TxnService interface {
	// Metadata returns the metadata of DNShard
	Metadata() metadata.DNShard
	// Start start the txn service
	Start() error
	// Close close the txn service
	Close() error
}

// TxnStorage txn storage
type TxnStorage interface {
}
