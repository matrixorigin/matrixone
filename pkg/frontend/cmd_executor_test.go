package frontend

import (
    "github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
    "github.com/stretchr/testify/assert"
    "testing"
)

func TestUserInput_getSqlSourceType(t *testing.T) {
    type fields struct {
        sql           string
        stmt          tree.Statement
        sqlSourceType []string
    }
    type args struct {
        i int
    }
    tests := []struct {
        name   string
        fields fields
        args   args
        want   string
    }{
        {
            name: "t1",
            fields: fields{
                sql:           "select * from t1",
                sqlSourceType: nil,
            },
            args: args{
                i: 0,
            },
            want: "external_sql",
        },
        {
            name: "t2",
            fields: fields{
                sql:           "select * from t1",
                sqlSourceType: nil,
            },
            args: args{
                i: 1,
            },
            want: "external_sql",
        },
        {
            name: "t3",
            fields: fields{
                sql: "select * from t1",
                sqlSourceType: []string{
                    "a",
                    "b",
                    "c",
                },
            },
            args: args{
                i: 2,
            },
            want: "c",
        },
    }
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            ui := &UserInput{
                sql:           tt.fields.sql,
                stmt:          tt.fields.stmt,
                sqlSourceType: tt.fields.sqlSourceType,
            }
            assert.Equalf(t, tt.want, ui.getSqlSourceType(tt.args.i), "getSqlSourceType(%v)", tt.args.i)
        })
    }
}
