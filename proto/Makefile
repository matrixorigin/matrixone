gen: plan.proto
	protoc --go_out=../pkg/pb/ ./plan.proto 

gen-logservice: logservice/rpc.proto logservice/heartbeat.proto
	protoc -I=.:../vendor --gogofaster_out=plugins=grpc:../pkg/pb/ logservice/*.proto

clean:
	rm *.h *.cc
