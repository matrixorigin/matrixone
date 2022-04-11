GEN = plan_generated.h

$(GEN): plan.fbs
		flatc --gen-mutable --reflect-names --cpp --scoped-enums plan.fbs
		flatc --gen-mutable --reflect-names --go --gen-onefile --go-namespace fbs -o ../pkg/fbs plan.fbs

clean:
	rm *.h *.go
