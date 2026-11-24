package realtime

import "sync"

type Peer interface{ SendText([]byte) error }

// Hub manages WebSocket peers inside one process. Cross-instance fan-out is
// intentionally not claimed; Redis PUBLISH is exposed separately for a future
// subscriber/bridge implementation.
type Hub struct {
	mu    sync.RWMutex
	rooms map[string]map[Peer]struct{}
}

func NewHub() *Hub { return &Hub{rooms: make(map[string]map[Peer]struct{})} }
func (h *Hub) Join(roomID string, p Peer) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.rooms[roomID] == nil {
		h.rooms[roomID] = make(map[Peer]struct{})
	}
	h.rooms[roomID][p] = struct{}{}
}
func (h *Hub) Leave(roomID string, p Peer) {
	h.mu.Lock()
	defer h.mu.Unlock()
	peers := h.rooms[roomID]
	delete(peers, p)
	if len(peers) == 0 {
		delete(h.rooms, roomID)
	}
}
func (h *Hub) Broadcast(roomID string, payload []byte) int {
	h.mu.RLock()
	peers := make([]Peer, 0, len(h.rooms[roomID]))
	for p := range h.rooms[roomID] {
		peers = append(peers, p)
	}
	h.mu.RUnlock()
	sent := 0
	for _, p := range peers {
		if p.SendText(payload) == nil {
			sent++
		}
	}
	return sent
}
func (h *Hub) Count(roomID string) int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.rooms[roomID])
}
