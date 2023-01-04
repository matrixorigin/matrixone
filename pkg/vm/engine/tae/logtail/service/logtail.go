// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package service

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/logtail"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
)

// Logtailer provides logtail for the specified table.
type Logtailer interface {
	// TableTotal returns full logtail for the specified table.
	TableTotal(
		ctx context.Context, table api.TableID, to timestamp.Timestamp,
	) (logtail.TableLogtail, error)

	// RangeTotal returns logtail for all tables within the range (from, to].
	RangeTotal(
		ctx context.Context, from, to timestamp.Timestamp,
	) ([]logtail.TableLogtail, error)

	// FetchLogtail returns logtail for the specified table.
	//
	// NOTE: If table not exist, logtail.TableLogtail shouldn't be a simple zero value.
	FetchLogtail(
		ctx context.Context, table api.TableID, from, to timestamp.Timestamp,
	) (logtail.TableLogtail, error)
}

// TODO: implement interface Logtailer
