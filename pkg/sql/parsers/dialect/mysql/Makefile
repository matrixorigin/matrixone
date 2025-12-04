# Note: All shift/reduce conflicts have been resolved.
# The build will fail if new conflicts are introduced (conflicts > 0).
mysql_sql.go: mysql_sql.y
	@output=$$(go run github.com/matrixorigin/matrixone/pkg/sql/parsers/goyacc -o mysql_sql.go mysql_sql.y 2>&1); \
	echo "$$output"; \
	conflicts=$$(echo "$$output" | grep "conflicts:" | sed -E 's/.*conflicts: ([0-9]+).*/\1/'); \
	if [ -z "$$conflicts" ]; then \
		conflicts=0; \
	fi; \
	if [ "$$conflicts" -gt 0 ]; then \
		rm -f mysql_sql.go; \
		echo "Error: New conflicts detected! Current conflicts: $$conflicts (expected max: 0)" >&2; \
		echo "To see conflict details, check y.output file or run:" >&2; \
		echo "  go run github.com/matrixorigin/matrixone/pkg/sql/parsers/goyacc -o /dev/null mysql_sql.y 2>&1 | grep -A 5 'shift/reduce conflict'" >&2; \
		exit 1; \
	fi; \
	if [ "$$conflicts" -gt 0 ]; then \
		echo "Warning: $$conflicts shift/reduce conflicts detected. Check y.output for details." >&2; \
		go run github.com/matrixorigin/matrixone/pkg/sql/parsers/goyacc -o /dev/null mysql_sql.y > y.output 2>&1; \
		echo "Conflict details:" >&2; \
		grep -n "shift/reduce conflict" y.output | head -10 >&2 || true; \
	fi
	gofmt -w mysql_sql.go
	rm -f y.output

clean:
	rm -f mysql_sql.go
	rm -f ../../scanner/tokens_mysql.go
