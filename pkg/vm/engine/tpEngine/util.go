package tpEngine

import "fmt"

func string2bytes(s string)[]byte{
	return []byte(s)
}

func bytes2string(bts []byte)string{
	return string(bts)
}

/**
check if x in a slice
*/
func isInSlice(x string,arr []string) bool {
	for _,y := range arr{
		if x == y {
			return true
		}
	}
	return false
}

//remove a string from array, return a new slice
func removeFromSlice(x string,arr []string)[]string {
	var i int
	var y string
	for i,y = range arr{
		if x == y {
			break
		}
	}

	if i < len(arr) {
		return append(arr[:i],arr[i+1:]...)
	}
	return arr
}

/**
make a byte slice with designated value
*/
func makeByteSlice(l int,v byte)[]byte{
	u := make([]byte,l)
	for i := 0; i < l; i += 1 {
		u[i] = v
	}
	return u
}

func sKey(num int, id string) string {
	return fmt.Sprintf("%v.%v", id, num)
}