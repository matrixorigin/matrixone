.PHONY: build
build: cmd/create/main.go cmd/load/main.go cmd/query/main.go
	go build -o create cmd/create/main.go
	go build -o load cmd/load/main.go
	go build -o query cmd/query/main.go

.PHONY: clean
clean:
	rm -f load
	rm -f query
	rm -f create
	rm -rf test.db
