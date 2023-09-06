// Copyright 2023 Matrix Origin
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

package backup

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMetas_AppendLaunchconfig(t *testing.T) {
	type fields struct {
		metas []*Meta
	}
	type args struct {
		subTyp string
		file   string
		check  func(t *testing.T, m *Metas)
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name:   "t1",
			fields: fields{},
			args: args{
				subTyp: "sub",
				file:   "t1",
				check: func(t *testing.T, m *Metas) {
					assert.Equal(t, 0, len(m.metas))
				},
			},
		},
		{
			name:   "t2",
			fields: fields{},
			args: args{
				subTyp: CnConfig,
				file:   "t2",
				check: func(t *testing.T, m *Metas) {
					assert.Equal(t, 1, len(m.metas))
					assert.Equal(t, CnConfig, m.metas[0].SubTyp)
					assert.Equal(t, "t2", m.metas[0].LaunchConfigFile)
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &Metas{
				metas: tt.fields.metas,
			}
			m.AppendLaunchconfig(tt.args.subTyp, tt.args.file)
			tt.args.check(t, m)
		})
	}
}
