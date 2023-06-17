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

package mocks

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
)

var testVectorPool *containers.VectorPool
var testRunTime *dbutils.Runtime

func init() {
	testVectorPool = containers.NewVectorPool("for-test", 20)
	testRunTime = dbutils.NewRuntime(
		dbutils.WithRuntimeMemtablePool(testVectorPool),
		dbutils.WithRuntimeTransientPool(testVectorPool),
	)
}

func GetTestVectorPool() *containers.VectorPool {
	return testVectorPool
}

func GetTestRunTime() *dbutils.Runtime {
	return testRunTime
}
