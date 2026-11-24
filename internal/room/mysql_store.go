package room

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
)

// MySQLStore is a database/sql repository adapter. The caller supplies an
// already-open *sql.DB with a MySQL driver registered. The practice server
// defaults to MemoryStore until the deployment wiring and DB integration tests
// are configured.
type MySQLStore struct{ db *sql.DB }

func NewMySQLStore(db *sql.DB) *MySQLStore { return &MySQLStore{db: db} }

func (s *MySQLStore) Create(in CreateInput) (Room, error) {
	title, host, max, err := validateCreateInput(in)
	if err != nil {
		return Room{}, err
	}
	id, err := newRoomID()
	if err != nil {
		return Room{}, err
	}
	now := time.Now().UTC()
	tx, err := s.db.Begin()
	if err != nil {
		return Room{}, err
	}
	defer tx.Rollback()
	_, err = tx.Exec(`INSERT INTO rooms(id,title,host_id,status,max_members,created_at) VALUES(?,?,?,?,?,?)`, id, title, host, string(StatusOpen), max, now)
	if err != nil {
		return Room{}, err
	}
	_, err = tx.Exec(`INSERT INTO room_members(room_id,user_id,role,joined_at) VALUES(?,?,?,?)`, id, host, "host", now)
	if err != nil {
		return Room{}, err
	}
	if err = tx.Commit(); err != nil {
		return Room{}, err
	}
	return s.Get(id)
}
func (s *MySQLStore) Get(id string) (Room, error) {
	r, err := scanRoom(s.db.QueryRow(`SELECT id,title,host_id,status,max_members,created_at FROM rooms WHERE id=?`, id))
	if err != nil {
		return Room{}, err
	}
	r.Members, err = s.loadMembers(id)
	return r, err
}
func (s *MySQLStore) List() []Room {
	rows, err := s.db.Query(`SELECT id,title,host_id,status,max_members,created_at FROM rooms ORDER BY created_at DESC`)
	if err != nil {
		return []Room{}
	}
	defer rows.Close()
	out := make([]Room, 0)
	for rows.Next() {
		r, e := scanRoom(rows)
		if e != nil {
			continue
		}
		r.Members, e = s.loadMembers(r.ID)
		if e == nil {
			out = append(out, r)
		}
	}
	return out
}
func (s *MySQLStore) Join(id, userID string) (Room, error) {
	userID = strings.TrimSpace(userID)
	if userID == "" {
		return Room{}, fmt.Errorf("user_id is required")
	}
	tx, err := s.db.Begin()
	if err != nil {
		return Room{}, err
	}
	defer tx.Rollback()
	r, err := scanRoom(tx.QueryRow(`SELECT id,title,host_id,status,max_members,created_at FROM rooms WHERE id=? FOR UPDATE`, id))
	if err != nil {
		return Room{}, err
	}
	if r.Status != StatusOpen {
		return Room{}, ErrClosed
	}
	var count int
	if err = tx.QueryRow(`SELECT COUNT(*) FROM room_members WHERE room_id=?`, id).Scan(&count); err != nil {
		return Room{}, err
	}
	if count >= r.MaxMembers {
		return Room{}, ErrFull
	}
	_, err = tx.Exec(`INSERT INTO room_members(room_id,user_id,role,joined_at) VALUES(?,?,?,?)`, id, userID, "member", time.Now().UTC())
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "duplicate") {
			return Room{}, ErrAlreadyMember
		}
		return Room{}, err
	}
	if err = tx.Commit(); err != nil {
		return Room{}, err
	}
	return s.Get(id)
}
func (s *MySQLStore) Leave(id, userID string) (Room, error) {
	userID = strings.TrimSpace(userID)
	if userID == "" {
		return Room{}, fmt.Errorf("user_id is required")
	}
	tx, err := s.db.Begin()
	if err != nil {
		return Room{}, err
	}
	defer tx.Rollback()
	r, err := scanRoom(tx.QueryRow(`SELECT id,title,host_id,status,max_members,created_at FROM rooms WHERE id=? FOR UPDATE`, id))
	if err != nil {
		return Room{}, err
	}
	if userID == r.HostID {
		return Room{}, errors.New("host cannot leave; close the room instead")
	}
	res, err := tx.Exec(`DELETE FROM room_members WHERE room_id=? AND user_id=?`, id, userID)
	if err != nil {
		return Room{}, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return Room{}, err
	}
	if n == 0 {
		return Room{}, ErrNotMember
	}
	if err = tx.Commit(); err != nil {
		return Room{}, err
	}
	return s.Get(id)
}
func (s *MySQLStore) Close(id, hostID string) (Room, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return Room{}, err
	}
	defer tx.Rollback()
	r, err := scanRoom(tx.QueryRow(`SELECT id,title,host_id,status,max_members,created_at FROM rooms WHERE id=? FOR UPDATE`, id))
	if err != nil {
		return Room{}, err
	}
	if strings.TrimSpace(hostID) != r.HostID {
		return Room{}, ErrNotHost
	}
	if _, err = tx.Exec(`UPDATE rooms SET status=? WHERE id=?`, string(StatusClosed), id); err != nil {
		return Room{}, err
	}
	if _, err = tx.Exec(`DELETE FROM room_members WHERE room_id=? AND user_id<>?`, id, r.HostID); err != nil {
		return Room{}, err
	}
	if err = tx.Commit(); err != nil {
		return Room{}, err
	}
	return s.Get(id)
}

type rowScanner interface{ Scan(dest ...any) error }

func scanRoom(row rowScanner) (Room, error) {
	var r Room
	var status string
	err := row.Scan(&r.ID, &r.Title, &r.HostID, &status, &r.MaxMembers, &r.CreatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Room{}, ErrNotFound
		}
		return Room{}, err
	}
	r.Status = Status(status)
	r.CreatedAt = r.CreatedAt.UTC()
	return r, nil
}
func (s *MySQLStore) loadMembers(id string) ([]string, error) {
	rows, err := s.db.Query(`SELECT user_id FROM room_members WHERE room_id=? ORDER BY role DESC, joined_at ASC`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	members := make([]string, 0)
	for rows.Next() {
		var user string
		if err = rows.Scan(&user); err != nil {
			return nil, err
		}
		members = append(members, user)
	}
	return members, rows.Err()
}
func newRoomID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}
