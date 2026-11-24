package realtime

import (
	"bufio"
	"encoding/base64"
	"encoding/binary"
	"strings"
	"testing"
)

type testPeer struct{ got [][]byte }

func (p *testPeer) SendText(b []byte) error {
	p.got = append(p.got, append([]byte(nil), b...))
	return nil
}

func maskedFrame(op byte, payload []byte) []byte {
	mask := []byte{1, 2, 3, 4}
	out := []byte{0x80 | op, 0x80 | byte(len(payload))}
	out = append(out, mask...)
	for i, b := range payload {
		out = append(out, b^mask[i%4])
	}
	return out
}
func TestReadClientFrame(t *testing.T) {
	payload := []byte(`{"type":"chat","content":"hello"}`)
	op, got, err := readClientFrame(bufio.NewReader(strings.NewReader(string(maskedFrame(1, payload)))))
	if err != nil {
		t.Fatal(err)
	}
	if op != 1 || string(got) != string(payload) {
		t.Fatalf("op=%d payload=%s", op, got)
	}
}
func TestReadClientFrameRejectsUnmasked(t *testing.T) {
	_, _, err := readClientFrame(bufio.NewReader(strings.NewReader("\x81\x02hi")))
	if err == nil {
		t.Fatal("expected unmasked frame rejection")
	}
}
func TestWebsocketAccept(t *testing.T) {
	got := websocketAccept("dGhlIHNhbXBsZSBub25jZQ==")
	want := "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="
	if got != want {
		t.Fatalf("got %s want %s", got, want)
	}
	decoded, err := base64.StdEncoding.DecodeString(got)
	if err != nil || len(decoded) != 20 {
		t.Fatalf("invalid accept: %v", err)
	}
}
func TestHubBroadcastAndLeave(t *testing.T) {
	h := NewHub()
	a, b := &testPeer{}, &testPeer{}
	h.Join("room-1", a)
	h.Join("room-1", b)
	if got := h.Broadcast("room-1", []byte("hello")); got != 2 {
		t.Fatalf("broadcast=%d", got)
	}
	if len(a.got) != 1 || len(b.got) != 1 {
		t.Fatal("both peers should receive message")
	}
	h.Leave("room-1", a)
	if h.Count("room-1") != 1 {
		t.Fatal("peer should be removed")
	}
	_ = binary.BigEndian // keep endian import explicit for related frame exercises
}
