mysql_sql.go: mysql_sql.y
	go run github.com/matrixorigin/matrixone/pkg/sql/parsers/goyacc -o mysql_sql.go -c mysql mysql_sql.y
	gofmt -w mysql_sql.go
	rm -f y.output
	mv tokens_mysql.go ../../scanner

clean:
	rm -f mysql_sql.go
	rm -f ../../scanner/tokens_mysql.go
