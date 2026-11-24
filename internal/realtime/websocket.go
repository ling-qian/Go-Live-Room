package realtime

import (
	"bufio"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const maxFrameSize = 64 * 1024
const websocketGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

type wsConn struct {
	conn    net.Conn
	reader  *bufio.Reader
	writeMu sync.Mutex
}

func (c *wsConn) SendText(payload []byte) error { return c.writeFrame(0x1, payload) }
func (c *wsConn) writeFrame(opcode byte, payload []byte) error {
	if len(payload) > maxFrameSize {
		return errors.New("websocket payload too large")
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	var header []byte
	switch n := len(payload); {
	case n < 126:
		header = []byte{0x80 | opcode, byte(n)}
	case n <= 65535:
		header = make([]byte, 4)
		header[0] = 0x80 | opcode
		header[1] = 126
		binary.BigEndian.PutUint16(header[2:], uint16(n))
	default:
		header = make([]byte, 10)
		header[0] = 0x80 | opcode
		header[1] = 127
		binary.BigEndian.PutUint64(header[2:], uint64(n))
	}
	if _, err := c.conn.Write(header); err != nil {
		return err
	}
	_, err := c.conn.Write(payload)
	return err
}

// ServeWebSocket upgrades a same-origin WebSocket connection and broadcasts
// text chat events to peers connected to the same in-process room.
func ServeWebSocket(w http.ResponseWriter, r *http.Request, roomID, userID string, hub *Hub) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if strings.TrimSpace(roomID) == "" || strings.TrimSpace(userID) == "" {
		http.Error(w, "room_id and user_id are required", http.StatusBadRequest)
		return
	}
	if !headerToken(r.Header.Get("Connection"), "upgrade") || !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
		http.Error(w, "websocket upgrade required", http.StatusBadRequest)
		return
	}
	if r.Header.Get("Sec-WebSocket-Version") != "13" {
		w.Header().Set("Sec-WebSocket-Version", "13")
		http.Error(w, "unsupported websocket version", http.StatusUpgradeRequired)
		return
	}
	if !sameOrigin(r) {
		http.Error(w, "origin not allowed", http.StatusForbidden)
		return
	}
	key := r.Header.Get("Sec-WebSocket-Key")
	decoded, err := base64.StdEncoding.DecodeString(key)
	if err != nil || len(decoded) != 16 {
		http.Error(w, "invalid websocket key", http.StatusBadRequest)
		return
	}
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "websocket not supported by response writer", http.StatusInternalServerError)
		return
	}
	conn, rw, err := hijacker.Hijack()
	if err != nil {
		return
	}
	accept := websocketAccept(key)
	response := "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: " + accept + "\r\n\r\n"
	if _, err = rw.WriteString(response); err == nil {
		err = rw.Flush()
	}
	if err != nil {
		conn.Close()
		return
	}
	peer := &wsConn{conn: conn, reader: rw.Reader}
	hub.Join(roomID, peer)
	defer func() { hub.Leave(roomID, peer); _ = peer.writeFrame(0x8, []byte{0x03, 0xE8}); _ = conn.Close() }()
	welcome, _ := json.Marshal(map[string]any{"type": "ready", "room_id": roomID, "user_id": userID, "at": time.Now().UTC()})
	_ = peer.SendText(welcome)
	for {
		opcode, payload, readErr := readClientFrame(peer.reader)
		if readErr != nil {
			return
		}
		switch opcode {
		case 0x1:
			var msg struct {
				Type    string `json:"type"`
				Content string `json:"content"`
			}
			if err := json.Unmarshal(payload, &msg); err != nil || msg.Type != "chat" || strings.TrimSpace(msg.Content) == "" {
				_ = peer.SendText([]byte(`{"type":"error","message":"expected chat JSON with non-empty content"}`))
				continue
			}
			if len(msg.Content) > 4000 {
				_ = peer.SendText([]byte(`{"type":"error","message":"content too long"}`))
				continue
			}
			out, _ := json.Marshal(map[string]any{"type": "message", "room_id": roomID, "user_id": userID, "content": msg.Content, "sent_at": time.Now().UTC()})
			hub.Broadcast(roomID, out)
		case 0x8:
			return
		case 0x9:
			_ = peer.writeFrame(0xA, payload)
		case 0xA:
			continue
		default:
			_ = peer.writeFrame(0x8, []byte{0x03, 0xEA})
			return
		}
	}
}

func websocketAccept(key string) string {
	sum := sha1.Sum([]byte(key + websocketGUID))
	return base64.StdEncoding.EncodeToString(sum[:])
}
func headerToken(value, token string) bool {
	for _, p := range strings.Split(value, ",") {
		if strings.EqualFold(strings.TrimSpace(p), token) {
			return true
		}
	}
	return false
}
func sameOrigin(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true
	}
	u, err := url.Parse(origin)
	if err != nil {
		return false
	}
	return strings.EqualFold(u.Host, r.Host) && (u.Scheme == "http" || u.Scheme == "https")
}

func readClientFrame(r *bufio.Reader) (byte, []byte, error) {
	var h [2]byte
	if _, err := io.ReadFull(r, h[:]); err != nil {
		return 0, nil, err
	}
	fin := h[0]&0x80 != 0
	if !fin || h[0]&0x70 != 0 {
		return 0, nil, errors.New("fragmented or reserved websocket frame unsupported")
	}
	opcode := h[0] & 0x0f
	masked := h[1]&0x80 != 0
	if !masked {
		return 0, nil, errors.New("client websocket frames must be masked")
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
		if length>>63 != 0 {
			return 0, nil, errors.New("invalid websocket payload length")
		}
	}
	if length > maxFrameSize {
		return 0, nil, fmt.Errorf("websocket payload exceeds %d bytes", maxFrameSize)
	}
	if (opcode == 0x8 || opcode == 0x9 || opcode == 0xA) && length > 125 {
		return 0, nil, errors.New("websocket control frame payload exceeds 125 bytes")
	}
	var mask [4]byte
	if _, err := io.ReadFull(r, mask[:]); err != nil {
		return 0, nil, err
	}
	payload := make([]byte, int(length))
	if _, err := io.ReadFull(r, payload); err != nil {
		return 0, nil, err
	}
	for i := range payload {
		payload[i] ^= mask[i%4]
	}
	return opcode, payload, nil
}
