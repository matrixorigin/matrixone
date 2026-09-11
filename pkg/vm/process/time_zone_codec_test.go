// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package process

import (
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestProcessCodecPreservesNamedTimeZoneRulesAcrossHops(t *testing.T) {
	oldLocal := time.Local
	time.Local = time.UTC
	defer func() { time.Local = oldLocal }()
	for _, test := range []struct{ name, winter, summer string }{
		{"Asia/Shanghai", "2024-01-01 08:00:00", "2024-07-01 08:00:00"},
		{"America/New_York", "2023-12-31 19:00:00", "2024-06-30 20:00:00"}} {
		t.Run(test.name, func(t *testing.T) {
			proc, _ := newCodecTestProcess(t)
			defer proc.Free()
			loc, err := time.LoadLocation(test.name)
			require.NoError(t, err)
			proc.Base.SessionInfo.TimeZone = loc
			for hop := 0; hop < 2; hop++ {
				info, err := proc.BuildProcessInfo("select group_concat(ts)")
				require.NoError(t, err)
				require.Equal(t, test.name, info.SessionInfo.TimeZoneName)
				data, err := info.Marshal()
				require.NoError(t, err)
				var wire pipeline.ProcessInfo
				require.NoError(t, wire.Unmarshal(data))
				session, err := ConvertToProcessSessionInfo(wire.SessionInfo)
				require.NoError(t, err)
				require.Equal(t, test.winter, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).In(session.TimeZone).Format("2006-01-02 15:04:05"))
				require.Equal(t, test.summer, time.Date(2024, 7, 1, 0, 0, 0, 0, time.UTC).In(session.TimeZone).Format("2006-01-02 15:04:05"))
				proc.Base.SessionInfo = session
			}
		})
	}
}
func TestProcessCodecLegacyOffsetAndInvalidNamedZone(t *testing.T) {
	proc, _ := newCodecTestProcess(t)
	defer proc.Free()
	info, err := proc.BuildProcessInfo("select 1")
	require.NoError(t, err)
	require.Empty(t, info.SessionInfo.TimeZoneName)
	session, err := ConvertToProcessSessionInfo(info.SessionInfo)
	require.NoError(t, err)
	_, offset := time.Date(2024, 7, 1, 0, 0, 0, 0, time.UTC).In(session.TimeZone).Zone()
	require.Equal(t, 8*3600, offset)
	for _, name := range []string{"No/Such_Time_Zone", "Local"} {
		info.SessionInfo.TimeZoneName = name
		_, err = ConvertToProcessSessionInfo(info.SessionInfo)
		require.Error(t, err)
	}
}
