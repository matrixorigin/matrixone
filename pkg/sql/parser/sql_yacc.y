//~/go/bin/goyacc -o sql_yacc.go sql_yacc.y
%{
package parser

type symUnion struct {
    val interface{}
}

%}

%token <str> TEXT_STRING 827

%union {
	str string
	union symUnion
}

%type <union> text_literal

%%
text_literal:
          TEXT_STRING
          {
          	$$.val = $1
          }
%%