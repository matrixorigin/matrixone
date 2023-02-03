// Copyright 2022 Matrix Origin
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

package metric

import (
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestConnectionCounter(t *testing.T) {
	type args struct {
		account string
	}
	tests := []struct {
		name string
		args args
		want float64
	}{
		{
			name: "count_1",
			args: args{account: "sys"},
			want: 1.0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := ConnectionCounter(tt.args.account)
			c.Inc()
			dtom := new(dto.Metric)
			c.Write(dtom)
			assert.Equal(t, tt.want, dtom.Gauge.GetValue())
		})
	}
}
