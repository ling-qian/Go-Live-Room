package room

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/ling-qian/Go-Live-Room/internal/cache"
	"github.com/ling-qian/Go-Live-Room/internal/realtime"
)

type Handler struct {
	store  Repository
	logger *slog.Logger
	redis  *cache.Client
	hub    *realtime.Hub
}

func NewHandler(store Repository, logger *slog.Logger, redisClient *cache.Client) http.Handler {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	h := &Handler{store: store, logger: logger, redis: redisClient, hub: realtime.NewHub()}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/rooms", h.create)
	mux.HandleFunc("GET /api/v1/rooms", h.list)
	mux.HandleFunc("GET /api/v1/rooms/{roomID}", h.get)
	mux.HandleFunc("POST /api/v1/rooms/{roomID}/join", h.join)
	mux.HandleFunc("POST /api/v1/rooms/{roomID}/leave", h.leave)
	mux.HandleFunc("POST /api/v1/rooms/{roomID}/close", h.close)
	mux.HandleFunc("GET /api/v1/rooms/{roomID}/ws", h.websocket)
	return h.requestLog(h.recover(mux))
}

func (h *Handler) create(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Title      string `json:"title"`
		HostID     string `json:"host_id"`
		MaxMembers int    `json:"max_members"`
	}
	if err := decodeJSON(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json", err.Error())
		return
	}
	created, err := h.store.Create(CreateInput{Title: req.Title, HostID: req.HostID, MaxMembers: req.MaxMembers})
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_room", err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"data": created})
}

func (h *Handler) list(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"data": h.store.List()})
}

func (h *Handler) get(w http.ResponseWriter, r *http.Request) {
	got, err := h.store.Get(r.PathValue("roomID"))
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": got})
}

func (h *Handler) join(w http.ResponseWriter, r *http.Request) {
	var req struct {
		UserID string `json:"user_id"`
	}
	if err := decodeJSON(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json", err.Error())
		return
	}
	updated, err := h.store.Join(r.PathValue("roomID"), req.UserID)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	h.updatePresence(r, updated.ID, strings.TrimSpace(req.UserID), true)
	writeJSON(w, http.StatusOK, map[string]any{"data": updated})
}

func (h *Handler) leave(w http.ResponseWriter, r *http.Request) {
	var req struct {
		UserID string `json:"user_id"`
	}
	if err := decodeJSON(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json", err.Error())
		return
	}
	updated, err := h.store.Leave(r.PathValue("roomID"), req.UserID)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	h.updatePresence(r, updated.ID, strings.TrimSpace(req.UserID), false)
	writeJSON(w, http.StatusOK, map[string]any{"data": updated})
}

func (h *Handler) close(w http.ResponseWriter, r *http.Request) {
	var req struct {
		HostID string `json:"host_id"`
	}
	if err := decodeJSON(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json", err.Error())
		return
	}
	closed, err := h.store.Close(r.PathValue("roomID"), req.HostID)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": closed})
}

func (h *Handler) websocket(w http.ResponseWriter, r *http.Request) {
	roomID := r.PathValue("roomID")
	userID := strings.TrimSpace(r.URL.Query().Get("user_id"))
	if userID == "" {
		writeError(w, http.StatusBadRequest, "user_required", "user_id is required")
		return
	}
	room, err := h.store.Get(roomID)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	if room.Status != StatusOpen {
		writeError(w, http.StatusConflict, "room_closed", "room is closed")
		return
	}
	realtime.ServeWebSocket(w, r, roomID, userID, h.hub)
}

func (h *Handler) updatePresence(r *http.Request, roomID, userID string, add bool) {
	if h.redis == nil || roomID == "" || userID == "" {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 800*time.Millisecond)
	defer cancel()
	key := "live:presence:" + roomID + ":" + userID
	var err error
	if add {
		err = h.redis.Set(ctx, key, "1", 30*time.Minute)
	} else {
		err = h.redis.Del(ctx, key)
	}
	if err != nil {
		h.logger.Warn("redis presence update failed", "room_id", roomID, "user_id", userID, "error", err)
	}
}

func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	defer r.Body.Close()
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return err
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("request must contain one JSON object")
		}
		return err
	}
	return nil
}

func writeStoreError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrNotFound):
		writeError(w, http.StatusNotFound, "not_found", err.Error())
	case errors.Is(err, ErrClosed):
		writeError(w, http.StatusConflict, "room_closed", err.Error())
	case errors.Is(err, ErrFull):
		writeError(w, http.StatusConflict, "room_full", err.Error())
	case errors.Is(err, ErrAlreadyMember):
		writeError(w, http.StatusConflict, "already_member", err.Error())
	case errors.Is(err, ErrNotMember):
		writeError(w, http.StatusConflict, "not_member", err.Error())
	case errors.Is(err, ErrNotHost):
		writeError(w, http.StatusForbidden, "not_host", err.Error())
	default:
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
	}
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]any{"error": map[string]string{"code": code, "message": message}})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func (h *Handler) requestLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		next.ServeHTTP(w, r)
		h.logger.Info("http request", "method", r.Method, "path", r.URL.Path, "duration_ms", time.Since(started).Milliseconds())
	})
}

func (h *Handler) recover(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if recovered := recover(); recovered != nil {
				h.logger.Error("panic recovered", "value", recovered, "path", r.URL.Path)
				writeError(w, http.StatusInternalServerError, "internal_error", "unexpected server error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// normalizeID is kept small and explicit to make request validation easy to test.
func normalizeID(value string) string { return strings.TrimSpace(value) }
