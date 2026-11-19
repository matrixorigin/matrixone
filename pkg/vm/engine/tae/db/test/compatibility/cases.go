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

package compatibility

import "testing"

func init() {

	PrepareCaseRegister(MakePrepareCase(
		prepareVector, "prepare-8", "prepare case append",
		schemaCfg{10, 2, 18, 13}, 10*3+1, longOpt,
	))
	TestCaseRegister(
		MakeTestCase(testVector, "prepare-8", "test-8", "prepare-8=>test-8"),
	)
}

func prepareVector(tc PrepareCase, t *testing.T) {
	tae := tc.GetEngine(t)
	defer tae.Close()

	bat := tc.GetBatch(t)
	defer bat.Close()

	tae.CreateRelAndAppend(bat, true)
}

func testVector(tc TestCase, t *testing.T) {
	pc := GetPrepareCase(tc.dependsOn)
	tae := initTestEngine(tc, t)
	defer tae.Close()

	bat := pc.GetBatch(t)
	defer bat.Close()

	tae.CheckRowsByScan(bat.Length(), true)
}
