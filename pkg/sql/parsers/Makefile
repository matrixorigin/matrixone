.PHONY: all
all: mysql postgresql

.PHONY: mysql
mysql:
	@cd dialect/mysql && make

.PHONY: postgresql
postgresql:
	@cd dialect/postgresql && make

.PHONY: clean
clean: 
	@cd dialect/mysql && make clean
	@cd dialect/postgresql && make clean
