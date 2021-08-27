# This Makefile is to build MatrixOne

UT_RESULT = ut_result.log

# Creating build config
.PHONY: config
config: cmd/generate-config/main.go cmd/generate-config/config_template.go cmd/generate-config/system_vars_def.toml
	go mod tidy
	go build -o gen_config cmd/generate-config/main.go cmd/generate-config/config_template.go
	./gen_config cmd/generate-config/system_vars_def.toml
	mv -f cmd/generate-config/system_vars_config.toml .
	mv -f cmd/generate-config/system_vars.go pkg/config
	mv -f cmd/generate-config/system_vars_test.go pkg/config

# Building mo-server binary
.PHONY: build
build: cmd/db-server/main.go
	go build -o mo-server cmd/db-server/main.go

# Building mo-server binary for debugging, it uses the latest MatrixCube from master.
.PHONY: debug
debug: cmd/db-server/main.go
	go get github.com/matrixorigin/matrixcube
	go mod tidy
	go build -tags debug -o mo-server cmd/db-server/main.go

# Running unit test cases
# Set test timeout to 15 minutes
# Excluding frontend test cases temporarily
.PHONY: test
UT_TIMEOUT = 15m
test:
	@if [[ -f $(UT_RESULT) ]]; then rm $(UT_RESULT); fi; \
	go vet ./pkg/...; \
	go clean -testcache && \
	go test -race -timeout $(UT_TIMEOUT) -v $$(go list ./... | egrep -v "frontend") | tee $(UT_RESULT) \

# Run mo-server
.PHONY: run
run: cmd/db-server/main.go
	go run cmd/db-server/main.go

# Tear down
.PHONY: clean
clean:
	go clean -testcache
	rm $(UT_RESULT)
	rm gen_config
	rm mo-server
