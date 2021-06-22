config: cmd/generate-config/main.go cmd/generate-config/system_vars_def.toml
	go build -o gen_config cmd/generate-config/main.go
	./gen_config cmd/generate-config/system_vars_def.toml
	mv -f cmd/generate-config/system_vars_config.toml .
	mv -f cmd/generate-config/system_vars.go pkg/config
	mv -f cmd/generate-config/system_vars_test.go pkg/config

build: cmd/db-server/main.go
	go build -o main cmd/db-server/main.go

run: cmd/db-server/main.go
	go run cmd/db-server/main.go

clean:
	rm gen_config
	rm main
