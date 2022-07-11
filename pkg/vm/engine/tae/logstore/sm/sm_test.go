package sm

import "testing"

func TestLoop1(t *testing.T) {
	q1:=make(chan any, 100)
	fn:=func(batch []any)any{
		for _,item:=range batch{
		t.Logf("loop1 %d",item.(int))
		}
		return nil
	}
	loop:=NewLoop(q1,nil,fn,100)
	loop.Start()
	for i:=0;i<10;i++{
		q1<-i
	}
	loop.Stop()
}