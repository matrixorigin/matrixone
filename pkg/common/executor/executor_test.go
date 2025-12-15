package executor

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExecutor(t *testing.T) {

	nthreads := 3
	vec := make([]int, 1024)
	answer := 0
	for i := range vec {
		vec[i] = i
		answer += i
	}

	e := NewExecutor(nthreads)

	r := make([]int, nthreads)

	err := e.Execute(len(vec), func(thread_id int, id int) error {
		r[thread_id] += vec[id]

		return nil
	})

	require.NoError(t, err)

	sum := 0
	for _, v := range r {
		sum += v
	}

	require.Equal(t, sum, answer)
}
