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

import (
	"fmt"
	"path"
)

type FileT int

const (
	FTLock FileT = iota
)

const (
	TmpSuffix  = ".tmp"
	LockSuffix = ".lock"
)

func MakeFilename(dirname string, ft FileT, name string, isTmp bool) string {
	var s string
	switch ft {
	case FTLock:
		s = path.Join(dirname, fmt.Sprintf("%s%s", name, LockSuffix))
	default:
		panic(fmt.Sprintf("unsupported %d", ft))
	}
	if isTmp {
		s += TmpSuffix
	}
	return s
}

func MakeLockFileName(dirname, name string) string {
	return MakeFilename(dirname, FTLock, name, false)
}
