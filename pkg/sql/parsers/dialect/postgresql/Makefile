postgresql_sql.go: postgresql_sql.y
	go run github.com/matrixorigin/matrixone/pkg/sql/parsers/goyacc -o postgresql_sql.go postgresql_sql.y
	gofmt -w postgresql_sql.go
	rm -f y.output

clean:
	rm -f postgresql_sql.go
