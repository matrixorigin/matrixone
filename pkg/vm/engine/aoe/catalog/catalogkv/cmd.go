package catalogkv

const (
	// write cmd
	set  uint64 = 1 // set
	incr uint64 = 2 // incr
	del  uint64 = 3 // delete
	bdel uint64 = 4 // batch delete
	rdel uint64 = 5 // range delete

	// read cmd
	get  uint64 = 10000
	mget uint64 = 10001
	scan uint64 = 10002
	seek uint64 = 10003
)
