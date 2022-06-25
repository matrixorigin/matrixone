postgresql_sql.go: postgresql_sql.y
	go run github.com/matrixorigin/matrixone/pkg/sql/parsers/goyacc -o postgresql_sql.go -c postgresql postgresql_sql.y
	gofmt -w postgresql_sql.go
	rm -f y.output
	mv tokens_postgresql.go ../../scanner

clean:
	rm -f postgresql_sql.go
	rm -f tokens_postgresql.go
