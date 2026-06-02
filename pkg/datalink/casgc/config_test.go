// Copyright 2024 Matrix Origin
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

package casgc

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestConfigAdjustDefaults(t *testing.T) {
	var c Config
	c.Adjust()
	require.Equal(t, time.Hour, c.Interval)
	require.Equal(t, 24*time.Hour, c.GraceWindow)
}

func TestConfigAdjustKeepsExplicit(t *testing.T) {
	c := Config{Interval: 5 * time.Minute, GraceWindow: 2 * time.Hour}
	c.Adjust()
	require.Equal(t, 5*time.Minute, c.Interval)
	require.Equal(t, 2*time.Hour, c.GraceWindow)
}
