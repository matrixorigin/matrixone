# Note: There are currently 7 shift/reduce conflicts that are pending resolution.
# The build will fail if new conflicts are introduced (conflicts > 7).
postgresql_sql.go: postgresql_sql.y
	@output=$$(go run github.com/matrixorigin/matrixone/pkg/sql/parsers/goyacc -o postgresql_sql.go postgresql_sql.y 2>&1); \
	echo "$$output"; \
	conflicts=$$(echo "$$output" | grep "conflicts:" | sed -E 's/.*conflicts: ([0-9]+).*/\1/'); \
	if [ -z "$$conflicts" ]; then \
		conflicts=0; \
	fi; \
	if [ "$$conflicts" -gt 7 ]; then \
		rm -f postgresql_sql.go; \
		echo "Error: New conflicts detected! Current conflicts: $$conflicts (expected max: 7)" >&2; \
		exit 1; \
	fi
	gofmt -w postgresql_sql.go
	rm -f y.output

clean:
	rm -f postgresql_sql.go
