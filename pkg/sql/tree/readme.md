Until now, I have only implemented the transformers in SelectStmt.

# usage
Parser is in parser.go

```
p := NewParser()
ast, err := p.Parse(sql)
```

# unsupported expression transformers that will be implemented in the future

CaseExpr{}  -> CaseExpr
DefaultExpr{} -> ColumnItem
PositionExpr{} -> OrderBy / GroupBy position

ValuesExpr{} -> ValuesClause  keyword: VALUES
VariableExpr{} -> system / session variables
           -> DDate / DTime / DTimestamp

# supported expression transformers
All transformers are in the file transformer.go
