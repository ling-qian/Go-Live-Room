package room

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func newTestServer() http.Handler { return NewHandler(NewStore(), nil, nil) }

func request(t *testing.T, h http.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var payload bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&payload).Encode(body); err != nil {
			t.Fatal(err)
		}
	}
	req := httptest.NewRequest(method, path, &payload)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	res := httptest.NewRecorder()
	h.ServeHTTP(res, req)
	return res
}

func decodeRoom(t *testing.T, res *httptest.ResponseRecorder) Room {
	t.Helper()
	var envelope struct {
		Data Room `json:"data"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("decode response: %v; body=%s", err, res.Body.String())
	}
	return envelope.Data
}

func TestCreateGetJoinAndLeaveRoom(t *testing.T) {
	h := newTestServer()
	created := request(t, h, http.MethodPost, "/api/v1/rooms", map[string]any{"title": "Engineering Live", "host_id": "host-1", "max_members": 3})
	if created.Code != http.StatusCreated {
		t.Fatalf("create status=%d body=%s", created.Code, created.Body.String())
	}
	r := decodeRoom(t, created)
	if r.ID == "" || len(r.Members) != 1 || r.Members[0] != "host-1" {
		t.Fatalf("unexpected room: %+v", r)
	}

	joined := request(t, h, http.MethodPost, "/api/v1/rooms/"+r.ID+"/join", map[string]string{"user_id": "guest-1"})
	if joined.Code != http.StatusOK {
		t.Fatalf("join status=%d body=%s", joined.Code, joined.Body.String())
	}
	if got := len(decodeRoom(t, joined).Members); got != 2 {
		t.Fatalf("member count=%d, want 2", got)
	}

	left := request(t, h, http.MethodPost, "/api/v1/rooms/"+r.ID+"/leave", map[string]string{"user_id": "guest-1"})
	if left.Code != http.StatusOK {
		t.Fatalf("leave status=%d body=%s", left.Code, left.Body.String())
	}
	if got := len(decodeRoom(t, left).Members); got != 1 {
		t.Fatalf("member count=%d, want 1", got)
	}

	got := request(t, h, http.MethodGet, "/api/v1/rooms/"+r.ID, nil)
	if got.Code != http.StatusOK {
		t.Fatalf("get status=%d", got.Code)
	}
}

func TestRoomCapacityIsEnforced(t *testing.T) {
	h := newTestServer()
	created := request(t, h, http.MethodPost, "/api/v1/rooms", map[string]any{"title": "Small Room", "host_id": "host", "max_members": 2})
	r := decodeRoom(t, created)
	first := request(t, h, http.MethodPost, "/api/v1/rooms/"+r.ID+"/join", map[string]string{"user_id": "guest"})
	if first.Code != http.StatusOK {
		t.Fatalf("first join status=%d", first.Code)
	}
	second := request(t, h, http.MethodPost, "/api/v1/rooms/"+r.ID+"/join", map[string]string{"user_id": "late"})
	if second.Code != http.StatusConflict {
		t.Fatalf("full room status=%d body=%s", second.Code, second.Body.String())
	}
}

func TestOnlyHostCanCloseRoom(t *testing.T) {
	h := newTestServer()
	created := request(t, h, http.MethodPost, "/api/v1/rooms", map[string]any{"title": "Room", "host_id": "host"})
	r := decodeRoom(t, created)
	denied := request(t, h, http.MethodPost, "/api/v1/rooms/"+r.ID+"/close", map[string]string{"host_id": "guest"})
	if denied.Code != http.StatusForbidden {
		t.Fatalf("non-host close status=%d", denied.Code)
	}
	closed := request(t, h, http.MethodPost, "/api/v1/rooms/"+r.ID+"/close", map[string]string{"host_id": "host"})
	if closed.Code != http.StatusOK {
		t.Fatalf("host close status=%d body=%s", closed.Code, closed.Body.String())
	}
	join := request(t, h, http.MethodPost, "/api/v1/rooms/"+r.ID+"/join", map[string]string{"user_id": "guest"})
	if join.Code != http.StatusConflict {
		t.Fatalf("join closed room status=%d", join.Code)
	}
}

func TestConcurrentJoinRespectsCapacity(t *testing.T) {
	h := newTestServer()
	created := request(t, h, http.MethodPost, "/api/v1/rooms", map[string]any{"title": "Concurrent Room", "host_id": "host", "max_members": 5})
	r := decodeRoom(t, created)

	const attempts = 30
	var wg sync.WaitGroup
	var mu sync.Mutex
	success := 0
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res := request(t, h, http.MethodPost, "/api/v1/rooms/"+r.ID+"/join", map[string]string{"user_id": fmt.Sprintf("user-%d", i)})
			if res.Code == http.StatusOK {
				mu.Lock()
				success++
				mu.Unlock()
			} else if res.Code != http.StatusConflict {
				t.Errorf("unexpected status %d: %s", res.Code, res.Body.String())
			}
		}(i)
	}
	wg.Wait()
	if success != 4 {
		t.Fatalf("successful joins=%d, want 4 (host occupies one slot)", success)
	}
	final := request(t, h, http.MethodGet, "/api/v1/rooms/"+r.ID, nil)
	if got := len(decodeRoom(t, final).Members); got != 5 {
		t.Fatalf("final member count=%d, want 5", got)
	}
}

func TestRejectsUnknownJSONFields(t *testing.T) {
	h := newTestServer()
	res := request(t, h, http.MethodPost, "/api/v1/rooms", map[string]any{"title": "Room", "host_id": "host", "extra": "not-allowed"})
	if res.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s", res.Code, res.Body.String())
	}
}

func maskWSFrame(opcode byte, payload []byte) []byte {
	mask := []byte{7, 11, 13, 17}
	frame := []byte{0x80 | opcode, 0x80 | byte(len(payload))}
	frame = append(frame, mask...)
	for i, b := range payload {
		frame = append(frame, b^mask[i%4])
	}
	return frame
}

func readServerWSFrame(r *bufio.Reader) (byte, []byte, error) {
	var h [2]byte
	if _, err := io.ReadFull(r, h[:]); err != nil {
		return 0, nil, err
	}
	if h[1]&0x80 != 0 {
		return 0, nil, fmt.Errorf("server frame must not be masked")
	}
	length := uint64(h[1] & 0x7f)
	if length == 126 {
		var ext [2]byte
		if _, err := io.ReadFull(r, ext[:]); err != nil {
			return 0, nil, err
		}
		length = uint64(binary.BigEndian.Uint16(ext[:]))
	}
	if length == 127 {
		var ext [8]byte
		if _, err := io.ReadFull(r, ext[:]); err != nil {
			return 0, nil, err
		}
		length = binary.BigEndian.Uint64(ext[:])
	}
	payload := make([]byte, int(length))
	_, err := io.ReadFull(r, payload)
	return h[0] & 0x0f, payload, err
}

func TestWebSocketRoomChatEndToEnd(t *testing.T) {
	store := NewStore()
	created, err := store.Create(CreateInput{Title: "WS Room", HostID: "host-1", MaxMembers: 10})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(NewHandler(store, nil, nil))
	defer server.Close()
	addr := strings.TrimPrefix(server.URL, "http://")
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	key := base64.StdEncoding.EncodeToString([]byte("0123456789abcdef"))
	request := fmt.Sprintf("GET /api/v1/rooms/%s/ws?user_id=guest-1 HTTP/1.1\r\nHost: %s\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\nOrigin: http://%s\r\n\r\n", created.ID, addr, key, addr)
	if _, err := io.WriteString(conn, request); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(conn)
	status, err := reader.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(status, "101 Switching Protocols") {
		t.Fatalf("unexpected handshake status: %s", status)
	}
	for {
		line, e := reader.ReadString('\n')
		if e != nil {
			t.Fatal(e)
		}
		if line == "\r\n" {
			break
		}
	}
	op, welcome, err := readServerWSFrame(reader)
	if err != nil {
		t.Fatal(err)
	}
	if op != 0x1 || !strings.Contains(string(welcome), `"type":"ready"`) {
		t.Fatalf("unexpected welcome frame: opcode=%d payload=%s", op, welcome)
	}
	chat := []byte(`{"type":"chat","content":"hello from test"}`)
	if _, err := conn.Write(maskWSFrame(0x1, chat)); err != nil {
		t.Fatal(err)
	}
	op, message, err := readServerWSFrame(reader)
	if err != nil {
		t.Fatal(err)
	}
	if op != 0x1 || !strings.Contains(string(message), `"content":"hello from test"`) {
		t.Fatalf("unexpected broadcast: opcode=%d payload=%s", op, message)
	}
	_, _ = conn.Write(maskWSFrame(0x8, []byte{0x03, 0xE8}))
}
