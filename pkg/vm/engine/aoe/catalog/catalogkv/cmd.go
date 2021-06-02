package catalogkv

const (
	// write cmd
	set  uint64 = 1 // set
	mset uint64 = 2 // multi set
	incr uint64 = 3 // incr
	del  uint64 = 4 // delete
	bdel uint64 = 5 // batch delete
	rdel uint64 = 6 // range delete

	// read cmd
	get  uint64 = 10000
	mget uint64 = 10001
	scan uint64 = 10002
	seek uint64 = 10003
)
