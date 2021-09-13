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

package tree

import (
	"log"
	//"reflect"
	"testing"
)

func TestParse(t *testing.T) {
	type args struct {
		sql string
	}

	_,s :=gen_transform_t15()

	sql :=`SELECT u.a,(SELECT t.a FROM sa.t,u)
		from u,(SELECT t.a,u.a FROM sa.t,u where t.a = u.a)
		where (u.a,u.b,u.c) in (SELECT t.a,u.a,t.b * u.b tubb
		FROM sa.t join u on t.c = u.c or t.d != u.d
				  join v on u.a != v.a
		where t.a = u.a and t.b > u.b
		group by t.a,u.a,(t.b+u.b+v.b)
		having t.a = 'jj' and v.c > 1000
		order by t.a asc,u.a desc,v.d asc,tubb
		limit 100,2000);`

	tests := []struct {
		name    string
		args    args
		want    []Statement
		wantErr bool
	}{
		{"t1",args{sql},[]Statement{s},false},
	}

	p := NewParser()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := p.Parse(tt.args.sql)
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			//if !reflect.DeepEqual(got, tt.want) {
			//	t.Errorf("Parse() got = %v, want %v", got, tt.want)
			//}
		})
	}
}

func TestParse2(t *testing.T) {
	sql :=`SELECT u.a,(SELECT t.a FROM sa.t,u)
		from u,(SELECT t.a,u.a FROM sa.t,u where t.a = u.a)
		where (u.a,u.b,u.c) in (SELECT t.a,u.a,t.b * u.b tubb
		FROM sa.t join u on t.c = u.c or t.d != u.d
				  join v on u.a != v.a
		where t.a = u.a and t.b > u.b
		group by t.a,u.a,(t.b+u.b+v.b)
		having t.a = 'jj' and v.c > 1000
		order by t.a asc,u.a desc,v.d asc,tubb
		limit 100,2000);`

	p := NewParser()
	_,err := p.Parse(sql)
	if err!=nil{
		log.Fatal(err)
	}
	//fmt.Printf("%s\n",stmt)
	//for _,st := range stmt{
	//	fmt.Printf("%s\n",st)
	//}
}

