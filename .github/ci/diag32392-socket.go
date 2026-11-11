package main

import (
	"fmt"
	"net"
	"os"
	"runtime"
	"syscall"
)

func describe(l *net.TCPListener) string {
	detail := l.Addr().String()
	raw, err := l.SyscallConn()
	if err != nil {
		return detail + " syscall=" + err.Error()
	}
	var only int
	var socketErr error
	err = raw.Control(func(fd uintptr) {
		only, socketErr = syscall.GetsockoptInt(int(fd), syscall.IPPROTO_IPV6, syscall.IPV6_V6ONLY)
	})
	if err != nil {
		return detail + " control=" + err.Error()
	}
	if socketErr != nil {
		return detail + " ipv6only=n/a"
	}
	return fmt.Sprintf("%s ipv6only=%d", detail, only)
}

func probe(network string, iteration int) (bool, error) {
	guard, err := net.Listen(network, "0.0.0.0:0")
	if err != nil {
		return false, err
	}
	defer guard.Close()
	g := guard.(*net.TCPListener)
	port := g.Addr().(*net.TCPAddr).Port
	// This matches memberlist's net.ListenTCP("tcp", tcpAddr) on wildcard IPv4.
	target, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.ParseIP("0.0.0.0"), Port: port})
	if err != nil {
		fmt.Printf("case=%s iteration=%d guard=%s target_error=%v\n", network, iteration, describe(g), err)
		return true, nil
	}
	defer target.Close()
	fmt.Printf("case=%s iteration=%d guard=%s target=%s both_listening=true\n", network, iteration, describe(g), describe(target))
	return false, nil
}

func main() {
	fmt.Printf("os=%s arch=%s go=%s\n", runtime.GOOS, runtime.GOARCH, runtime.Version())
	for _, network := range []string{"tcp4", "tcp"} {
		for i := 0; i < 3; i++ {
			collided, err := probe(network, i)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			if network == "tcp" && !collided {
				fmt.Fprintln(os.Stderr, "same-network wildcard listener failed to cause collision")
				os.Exit(1)
			}
		}
	}
}
