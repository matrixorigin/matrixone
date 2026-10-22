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

package plugin

import (
	"testing"

	"github.com/stretchr/testify/require"

	searchplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/search"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type searchHooks struct{}

func (searchHooks) NewReader(*process.Process, *plan.IndexSearchScan, searchplugin.Request) (engine.Reader, error) {
	return nil, nil
}

type membershipHooks struct {
	searchHooks
	supports bool
}

func (h membershipHooks) SupportsMembership() bool { return h.supports }

type probeKnobsHooks struct {
	searchHooks
	supports bool
}

func (h probeKnobsHooks) SupportsProbeKnobs() bool { return h.supports }

type searchAlgo struct {
	bareAlgo
	hooks searchplugin.Hooks
}

func (p searchAlgo) Search() searchplugin.Hooks { return p.hooks }

func TestSupportsMembership(t *testing.T) {
	registerForTest(t, bareAlgo{algo: "membership_bare"})
	registerForTest(t, searchAlgo{bareAlgo: bareAlgo{algo: "membership_search"}, hooks: searchHooks{}})
	registerForTest(t, searchAlgo{bareAlgo: bareAlgo{algo: "membership_no"}, hooks: membershipHooks{supports: false}})
	registerForTest(t, searchAlgo{bareAlgo: bareAlgo{algo: "membership_yes"}, hooks: membershipHooks{supports: true}})

	require.False(t, SupportsMembership("membership_unregistered"))
	require.False(t, SupportsMembership("membership_bare"))
	require.False(t, SupportsMembership("membership_search"))
	require.False(t, SupportsMembership("membership_no"))
	require.True(t, SupportsMembership("membership_yes"))
	require.True(t, SupportsMembership(" MEMBERSHIP_YES "))
}

func TestSupportsProbeKnobs(t *testing.T) {
	registerForTest(t, bareAlgo{algo: "probe_bare"})
	registerForTest(t, searchAlgo{bareAlgo: bareAlgo{algo: "probe_search"}, hooks: searchHooks{}})
	registerForTest(t, searchAlgo{bareAlgo: bareAlgo{algo: "probe_no"}, hooks: probeKnobsHooks{supports: false}})
	registerForTest(t, searchAlgo{bareAlgo: bareAlgo{algo: "probe_yes"}, hooks: probeKnobsHooks{supports: true}})

	require.False(t, SupportsProbeKnobs("probe_unregistered"))
	require.False(t, SupportsProbeKnobs("probe_bare"))
	require.False(t, SupportsProbeKnobs("probe_search"))
	require.False(t, SupportsProbeKnobs("probe_no"))
	require.True(t, SupportsProbeKnobs("probe_yes"))
}
