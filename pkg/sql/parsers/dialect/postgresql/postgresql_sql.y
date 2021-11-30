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

%{
package postgresql
    
import (
    "github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)
%}

%struct {
    id  int
    str string
    item interface{}
}

%union {
    statement tree.Statement
    statements []tree.Statement
}

%token <str> USE ID
%token <str> INTEGRAL FLOAT HEX HEXNUM BIT_LITERAL

%type <statement> stmt
%type <statements> stmt_list
%type <statement> use_stmt

%start start_command

%%

start_command:
    stmt_list

stmt_list:
    stmt
    {
        if $1 != nil {
            yylex.(*Lexer).AppendStmt($1)
        }
    }
|   stmt_list ';' stmt
    {
        if $3 != nil {
            yylex.(*Lexer).AppendStmt($3)
        }
    }

stmt:
   use_stmt 

use_stmt:
    USE ID
    {
        $$ = &tree.Use{Name: $2}
    }
|   USE
    {
        $$ = &tree.Use{}
    }
%%
