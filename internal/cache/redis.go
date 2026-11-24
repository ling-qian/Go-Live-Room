package cache

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
)

// Client is a small RESP2 client used by this practice service. It opens one
// connection per command to keep connection ownership simple and race-safe.
type Client struct {
	Addr     string
	Password string
	DB       int
	Timeout  time.Duration
}

func New(addr, password string, db int) *Client {
	return &Client{Addr: addr, Password: password, DB: db, Timeout: 2 * time.Second}
}

func (c *Client) Ping(ctx context.Context) error {
	v, err := c.do(ctx, "PING")
	if err != nil {
		return err
	}
	if v != "PONG" {
		return fmt.Errorf("unexpected redis PING response: %v", v)
	}
	return nil
}

func (c *Client) Set(ctx context.Context, key, value string, ttl time.Duration) error {
	if key == "" {
		return errors.New("redis key is required")
	}
	args := []string{"SET", key, value}
	if ttl > 0 {
		args = append(args, "PX", strconv.FormatInt(ttl.Milliseconds(), 10))
	}
	_, err := c.do(ctx, args...)
	return err
}

func (c *Client) Get(ctx context.Context, key string) (string, bool, error) {
	v, err := c.do(ctx, "GET", key)
	if err != nil {
		return "", false, err
	}
	if v == nil {
		return "", false, nil
	}
	s, ok := v.(string)
	if !ok {
		return "", false, fmt.Errorf("unexpected redis GET response type %T", v)
	}
	return s, true, nil
}

func (c *Client) Del(ctx context.Context, key string) error {
	_, err := c.do(ctx, "DEL", key)
	return err
}

func (c *Client) Publish(ctx context.Context, channel, message string) (int64, error) {
	v, err := c.do(ctx, "PUBLISH", channel, message)
	if err != nil {
		return 0, err
	}
	n, ok := v.(int64)
	if !ok {
		return 0, fmt.Errorf("unexpected redis PUBLISH response type %T", v)
	}
	return n, nil
}

func (c *Client) do(ctx context.Context, args ...string) (any, error) {
	if c.Addr == "" {
		return nil, errors.New("redis address is empty")
	}
	timeout := c.Timeout
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	d := net.Dialer{Timeout: timeout}
	conn, err := d.DialContext(ctx, "tcp", c.Addr)
	if err != nil {
		return nil, fmt.Errorf("dial redis: %w", err)
	}
	defer conn.Close()
	deadline := time.Now().Add(timeout)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}
	_ = conn.SetDeadline(deadline)
	reader := bufio.NewReader(conn)
	if c.Password != "" {
		if err := writeCommand(conn, "AUTH", c.Password); err != nil {
			return nil, err
		}
		if _, err := readRESP(reader); err != nil {
			return nil, fmt.Errorf("redis AUTH: %w", err)
		}
	}
	if c.DB != 0 {
		if err := writeCommand(conn, "SELECT", strconv.Itoa(c.DB)); err != nil {
			return nil, err
		}
		if _, err := readRESP(reader); err != nil {
			return nil, fmt.Errorf("redis SELECT: %w", err)
		}
	}
	if err := writeCommand(conn, args...); err != nil {
		return nil, err
	}
	return readRESP(reader)
}

func writeCommand(w io.Writer, args ...string) error {
	if len(args) == 0 {
		return errors.New("empty redis command")
	}
	var b strings.Builder
	fmt.Fprintf(&b, "*%d\r\n", len(args))
	for _, arg := range args {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(arg), arg)
	}
	_, err := io.WriteString(w, b.String())
	return err
}

func readRESP(r *bufio.Reader) (any, error) {
	prefix, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
	switch prefix {
	case '+':
		return line, nil
	case '-':
		return nil, fmt.Errorf("redis error: %s", line)
	case ':':
		n, e := strconv.ParseInt(line, 10, 64)
		return n, e
	case '$':
		n, e := strconv.Atoi(line)
		if e != nil {
			return nil, e
		}
		if n == -1 {
			return nil, nil
		}
		if n < 0 || n > 4*1024*1024 {
			return nil, fmt.Errorf("invalid redis bulk length %d", n)
		}
		buf := make([]byte, n+2)
		if _, e = io.ReadFull(r, buf); e != nil {
			return nil, e
		}
		if string(buf[n:]) != "\r\n" {
			return nil, errors.New("invalid redis bulk terminator")
		}
		return string(buf[:n]), nil
	case '*':
		n, e := strconv.Atoi(line)
		if e != nil {
			return nil, e
		}
		if n == -1 {
			return nil, nil
		}
		if n < 0 || n > 1024 {
			return nil, fmt.Errorf("invalid redis array length %d", n)
		}
		items := make([]any, n)
		for i := range items {
			items[i], e = readRESP(r)
			if e != nil {
				return nil, e
			}
		}
		return items, nil
	default:
		return nil, fmt.Errorf("unsupported redis RESP prefix %q", prefix)
	}
}
