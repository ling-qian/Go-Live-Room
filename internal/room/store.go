package room

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

var (
	ErrNotFound      = errors.New("room not found")
	ErrClosed        = errors.New("room is closed")
	ErrFull          = errors.New("room is full")
	ErrAlreadyMember = errors.New("user already joined")
	ErrNotMember     = errors.New("user is not a member")
	ErrNotHost       = errors.New("only the host can close the room")
)

type Status string

const (
	StatusOpen   Status = "open"
	StatusClosed Status = "closed"
)

type Room struct {
	ID         string    `json:"id"`
	Title      string    `json:"title"`
	HostID     string    `json:"host_id"`
	Status     Status    `json:"status"`
	MaxMembers int       `json:"max_members"`
	Members    []string  `json:"members"`
	CreatedAt  time.Time `json:"created_at"`
}

type CreateInput struct {
	Title      string
	HostID     string
	MaxMembers int
}

type Repository interface {
	Create(CreateInput) (Room, error)
	Get(string) (Room, error)
	List() []Room
	Join(string, string) (Room, error)
	Leave(string, string) (Room, error)
	Close(string, string) (Room, error)
}

func validateCreateInput(in CreateInput) (title, hostID string, maxMembers int, err error) {
	title = strings.TrimSpace(in.Title)
	hostID = strings.TrimSpace(in.HostID)
	if title == "" || hostID == "" {
		return "", "", 0, fmt.Errorf("title and host_id are required")
	}
	maxMembers = in.MaxMembers
	if maxMembers == 0 {
		maxMembers = 100
	}
	if maxMembers < 2 || maxMembers > 1000 {
		return "", "", 0, fmt.Errorf("max_members must be between 2 and 1000")
	}
	return title, hostID, maxMembers, nil
}

type Store struct {
	mu    sync.RWMutex
	rooms map[string]*Room
	next  uint64
}

func NewStore() *Store {
	return &Store{rooms: make(map[string]*Room)}
}

func (s *Store) Create(in CreateInput) (Room, error) {
	title, hostID, maxMembers, err := validateCreateInput(in)
	if err != nil {
		return Room{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.next++
	id := fmt.Sprintf("room-%06d", s.next)
	r := &Room{
		ID: id, Title: title, HostID: hostID, Status: StatusOpen,
		MaxMembers: maxMembers, Members: []string{hostID}, CreatedAt: time.Now().UTC(),
	}
	s.rooms[id] = r
	return cloneRoom(r), nil
}

func (s *Store) Get(id string) (Room, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r, ok := s.rooms[id]
	if !ok {
		return Room{}, ErrNotFound
	}
	return cloneRoom(r), nil
}

func (s *Store) List() []Room {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Room, 0, len(s.rooms))
	for _, r := range s.rooms {
		out = append(out, cloneRoom(r))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out
}

func (s *Store) Join(id, userID string) (Room, error) {
	userID = strings.TrimSpace(userID)
	if userID == "" {
		return Room{}, fmt.Errorf("user_id is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.rooms[id]
	if !ok {
		return Room{}, ErrNotFound
	}
	if r.Status != StatusOpen {
		return Room{}, ErrClosed
	}
	for _, member := range r.Members {
		if member == userID {
			return Room{}, ErrAlreadyMember
		}
	}
	if len(r.Members) >= r.MaxMembers {
		return Room{}, ErrFull
	}
	r.Members = append(r.Members, userID)
	return cloneRoom(r), nil
}

func (s *Store) Leave(id, userID string) (Room, error) {
	userID = strings.TrimSpace(userID)
	if userID == "" {
		return Room{}, fmt.Errorf("user_id is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.rooms[id]
	if !ok {
		return Room{}, ErrNotFound
	}
	for i, member := range r.Members {
		if member == userID {
			if userID == r.HostID {
				return Room{}, fmt.Errorf("host cannot leave; close the room instead")
			}
			r.Members = append(r.Members[:i], r.Members[i+1:]...)
			return cloneRoom(r), nil
		}
	}
	return Room{}, ErrNotMember
}

func (s *Store) Close(id, hostID string) (Room, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.rooms[id]
	if !ok {
		return Room{}, ErrNotFound
	}
	if strings.TrimSpace(hostID) != r.HostID {
		return Room{}, ErrNotHost
	}
	r.Status = StatusClosed
	r.Members = []string{r.HostID}
	return cloneRoom(r), nil
}

func cloneRoom(r *Room) Room {
	copyOf := *r
	copyOf.Members = append([]string(nil), r.Members...)
	return copyOf
}
