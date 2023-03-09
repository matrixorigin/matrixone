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

%token LEX_ERROR
%nonassoc EMPTY
%left <str> UNION
%token <str> SELECT STREAM INSERT UPDATE DELETE FROM WHERE GROUP HAVING ORDER BY LIMIT OFFSET FOR
%nonassoc LOWER_THAN_SET
%nonassoc <str> SET
%token <str> ALL DISTINCT DISTINCTROW AS EXISTS ASC DESC INTO DUPLICATE DEFAULT LOCK KEYS
%token <str> VALUES
%token <str> NEXT VALUE SHARE MODE
%token <str> SQL_NO_CACHE SQL_CACHE
%left <str> JOIN STRAIGHT_JOIN LEFT RIGHT INNER OUTER CROSS NATURAL USE FORCE
%left <str> ON USING
%left <str> SUBQUERY_AS_EXPR
%left <str> '(' ',' ')'
%nonassoc LOWER_THAN_STRING
%nonassoc <str> ID AT_ID AT_AT_ID STRING VALUE_ARG LIST_ARG COMMENT COMMENT_KEYWORD
%token <item> INTEGRAL HEX BIT_LITERAL FLOAT HEXNUM
%token <str> NULL TRUE FALSE
%nonassoc LOWER_THAN_CHARSET
%nonassoc <str> CHARSET
%right <str> UNIQUE KEY
%left <str> OR PIPE_CONCAT
%left <str> XOR
%left <str> AND
%right <str> NOT '!'
%left <str> BETWEEN CASE WHEN THEN ELSE END
%left <str> '=' '<' '>' LE GE NE NULL_SAFE_EQUAL IS LIKE REGEXP IN ASSIGNMENT
%left <str> '|'
%left <str> '&'
%left <str> SHIFT_LEFT SHIFT_RIGHT
%left <str> '+' '-'
%left <str> '*' '/' DIV '%' MOD
%left <str> '^'
%right <str> '~' UNARY
%left <str> COLLATE
%right <str> BINARY UNDERSCORE_BINARY
%right <str> INTERVAL
%nonassoc <str> '.'

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
        $$ = &tree.Use{Name: tree.NewCStr($2, 1)}
    }
|   USE
    {
        $$ = &tree.Use{}
    }
%%
