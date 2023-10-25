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

package common

import "time"

type DurationRecords struct {
	Duration time.Duration
}

type RecordCtxKeyType string

var ActiveHandleCommit RecordCtxKeyType = "Active Handle Commit"
var ActiveHandleRequests RecordCtxKeyType = "Active Handle Requests"

var DequeuePreparing RecordCtxKeyType = "Dequeue Preparing"
var PrepareWAL RecordCtxKeyType = "Prepare WAL"
var DequeuePrepared RecordCtxKeyType = "Dequeue Prepared"

var PrepareLogtail RecordCtxKeyType = "Prepare Logtail"
var StorePrePrepare RecordCtxKeyType = "Store Pre Prepare"
var StorePrepareCommit RecordCtxKeyType = "Store Prepare Commit"
var StorePreApplyCommit RecordCtxKeyType = "Store Pre Apply Commit"
var StoreApplyCommit RecordCtxKeyType = "Store Apply Commit"
