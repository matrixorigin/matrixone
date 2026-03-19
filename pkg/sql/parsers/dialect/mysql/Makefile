# Note: There are currently 7 shift/reduce conflicts that are pending resolution.
# The build will fail if new conflicts are introduced (conflicts > 7).
mysql_sql.go: mysql_sql.y
	@output=$$(go run github.com/matrixorigin/matrixone/pkg/sql/parsers/goyacc -o mysql_sql.go mysql_sql.y 2>&1); \
	echo "$$output"; \
	conflicts=$$(echo "$$output" | grep "conflicts:" | sed -E 's/.*conflicts: ([0-9]+).*/\1/'); \
	if [ -z "$$conflicts" ]; then \
		conflicts=0; \
	fi; \
	if [ "$$conflicts" -gt 7 ]; then \
		rm -f mysql_sql.go; \
		echo "Error: New conflicts detected! Current conflicts: $$conflicts (expected max: 7)" >&2; \
		exit 1; \
	fi
	gofmt -w mysql_sql.go
	rm -f y.output

clean:
	rm -f mysql_sql.go
	rm -f ../../scanner/tokens_mysql.go
