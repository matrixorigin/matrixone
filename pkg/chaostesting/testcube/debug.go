package main

import (
	"net/http"
	_ "net/http/pprof"
)

func init() {
	go func() {
		http.ListenAndServe(":8888", nil)
	}()
}
