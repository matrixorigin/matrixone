gen: plan.proto
	protoc --go_out=../pkg/pb/ ./plan.proto 
	protoc --go_out=../pkg/pb/ ./txn.proto 
	protoc --go_out=../pkg/pb/ ./timestamp.proto 

clean:
	rm *.h *.cc
