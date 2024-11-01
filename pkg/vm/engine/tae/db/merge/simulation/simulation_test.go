package simulation

import (
	"github.com/stretchr/testify/require"
	"math/rand/v2"
	"os"
	"testing"
	"time"
)

func TestSimulation(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "example")
	require.NoError(t, err)

	defer os.Remove(tmpFile.Name()) // clean up

	_, err = tmpFile.Write([]byte("w\n"))
	require.NoError(t, err)

	_, err = tmpFile.Write([]byte("a\n"))
	require.NoError(t, err)

	_, err = tmpFile.Write([]byte("p\n"))
	require.NoError(t, err)

	_, err = tmpFile.Write([]byte("q\n"))
	require.NoError(t, err)

	_, err = tmpFile.Seek(0, 0)
	require.NoError(t, err)

	oldStdin := os.Stdin
	defer func() { os.Stdin = oldStdin }() // Restore original Stdin

	os.Stdin = tmpFile

	sim(10000, 100*time.Millisecond, func() time.Duration {
		return time.Duration(rand.ExpFloat64()*100) * time.Millisecond
	}, false)
}
