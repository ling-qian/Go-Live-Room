package cache

import (
	"bufio"
	"context"
	"net"
	"strings"
	"testing"
	"time"
)

func TestWriteCommand(t *testing.T) {
	var b strings.Builder
	if err := writeCommand(&b, "SET", "room:1", "open", "PX", "30000"); err != nil {
		t.Fatal(err)
	}
	want := "*5\r\n$3\r\nSET\r\n$6\r\nroom:1\r\n$4\r\nopen\r\n$2\r\nPX\r\n$5\r\n30000\r\n"
	if b.String() != want {
		t.Fatalf("got %q want %q", b.String(), want)
	}
}
func TestReadRESP(t *testing.T) {
	cases := []struct {
		input string
		want  any
	}{{"+PONG\r\n", "PONG"}, {":12\r\n", int64(12)}, {"$3\r\nabc\r\n", "abc"}, {"$-1\r\n", nil}}
	for _, tc := range cases {
		got, err := readRESP(bufio.NewReader(strings.NewReader(tc.input)))
		if err != nil {
			t.Fatal(err)
		}
		if got != tc.want {
			t.Fatalf("input %q got %#v want %#v", tc.input, got, tc.want)
		}
	}
}

func TestClientPingAgainstRESPMockServer(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	serverErr := make(chan error, 1)
	go func() {
		conn, e := listener.Accept()
		if e != nil {
			serverErr <- e
			return
		}
		defer conn.Close()
		_, e = readRESP(bufio.NewReader(conn))
		if e != nil {
			serverErr <- e
			return
		}
		_, e = conn.Write([]byte("+PONG\r\n"))
		serverErr <- e
	}()
	client := New(listener.Addr().String(), "", 0)
	client.Timeout = time.Second
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := client.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
}
