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

package db

import (
	"errors"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
)

var (
	ErrClosed            = errors.New("aoe: closed")
	ErrUnsupported       = errors.New("aoe: unsupported")
	ErrNotFound          = errors.New("aoe: notfound")
	ErrUnexpectedWalRole = errors.New("aoe: unexpected wal role setted")
	ErrTimeout           = errors.New("aoe: timeout")
	ErrStaleErr          = errors.New("aoe: stale")
	ErrIdempotence       = metadata.ErrIdempotence
	ErrResourceDeleted   = errors.New("aoe: resource is deleted")
)
