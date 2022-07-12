package bytejson

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestLiteral(t *testing.T) {
	j := []string{"true", "false", "null"}
	for _, x := range j {
		bj, err := ParseFromString(x)
		require.Nil(t, err)
		require.Equal(t, x, bj.String())
	}
}

//func TestMap(t *testing.T) {
//	mp := make(map[int]string)
//	mp[1] = "1"
//	mp[2] = "2"
//	mp[3] = "3"
//	mp[4] = "3131314"
//	require.Equal(t, len(mp), 4)
//}

//func TestName(t *testing.T) {
//	for i := 0; i < 10; i++ {
//		rd := rand.Int() % 99782309
//		now := make([]byte, rd)
//		for i := 0; i < rd; i++ {
//			require.Equal(t, byte(0), now[i])
//		}
//	}
//}

//func BenchmarkAddZero(b *testing.B) {
//
//	for i := 0; i < b.N; i++ {
//		addZero(nil, rand.Int()%10241)
//	}
//}
//func BenchmarkAddZero2(b *testing.B) {
//	for i := 0; i < b.N; i++ {
//		addZero2(nil, rand.Int()%10241)
//	}
//}
