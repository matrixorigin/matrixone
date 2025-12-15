package executor

import (
	"errors"
	"sync"
)

type Executor struct {
	nthreads int
}

func NewExecutor(nthreads int) Executor {
	return Executor{nthreads: nthreads}
}

func (e Executor) Execute(nitems int, fn func(thread_id int, item_id int) error) (err error) {
	var wg sync.WaitGroup
	errs := make(chan error, e.nthreads)

	for i := 0; i < e.nthreads; i++ {
		wg.Add(1)
		go func(thread_id int) {
			defer wg.Done()

			for j := 0; j < nitems; j++ {
				if j%e.nthreads != thread_id {
					continue
				}
				err2 := fn(thread_id, j)
				if err2 != nil {
					errs <- err2
					return
				}
			}

		}(i)
	}

	wg.Wait()

	if len(errs) > 0 {
		for i := 0; i < len(errs); i++ {
			err = errors.Join(err, <-errs)
		}
	}
	return

}
