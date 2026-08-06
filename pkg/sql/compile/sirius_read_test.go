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

package compile

import (
	"context"
	"testing"

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/substrait"
	"github.com/stretchr/testify/require"
)

type siriusJournalStub struct{}

func (siriusJournalStub) Store(context.Context, *substrait.Lease) error { return nil }
func (siriusJournalStub) MarkReleased(context.Context, []byte) error    { return nil }
func (siriusJournalStub) Delete(context.Context, []byte) error          { return nil }
func (siriusJournalStub) Load(context.Context) ([]*substrait.Lease, error) {
	return nil, nil
}

func TestSiriusReadPlanRelease(t *testing.T) {
	ctx := context.Background()
	require.NoError(t, (*SiriusReadPlan)(nil).Release(ctx, nil))
	plan := &SiriusReadPlan{ReadRefs: [][]byte{[]byte("one"), []byte("two")}}
	require.NoError(t, plan.Release(ctx, substrait.NewLeaseManager(1, nil)))

	notReplayed := substrait.NewPersistentLeaseManager(1, nil, siriusJournalStub{})
	require.ErrorContains(t, plan.Release(ctx, notReplayed), "not been replayed")
}

func TestCompileSiriusReadRejectsMissingPlan(t *testing.T) {
	ctx := context.Background()
	var c *Compile
	_, err := c.CompileSiriusRead(ctx, nil, 0, nil, nil, "", 0, nil)
	require.ErrorContains(t, err, "no query plan")
	_, err = c.CompileSiriusRead(ctx, &planpb.Plan{}, 0, nil, nil, "", 0, nil)
	require.ErrorContains(t, err, "no query plan")

	c = &Compile{}
	invalid := &planpb.Plan{Plan: &planpb.Plan_Query{Query: &planpb.Query{StmtType: planpb.Query_SELECT}}}
	_, err = c.CompileSiriusRead(ctx, invalid, 0, nil, nil, "", 0, nil)
	require.Error(t, err)
}
