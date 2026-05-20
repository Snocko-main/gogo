//go:build cgo && gogo

package gogo_test

import (
	"bufio"
	"crypto/rand"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"
)

// Minimal WebSocket client for tests — RFC 6455 handshake + frame
// encode/decode. We avoid pulling gorilla/websocket as a test
// dependency since this is all we need for pub/sub round-trip checks.

const wsAcceptGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

// wsClient is a half-duplex test WebSocket client. Read returns the
// payload of the next text/binary frame; control frames (ping/close)
// are handled internally so tests don't see them.
type wsClient struct {
	conn net.Conn
	br   *bufio.Reader
}

func dialWebSocket(port int, path string) (*wsClient, error) {
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 5*time.Second)
	if err != nil {
		return nil, fmt.Errorf("dial: %w", err)
	}
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		conn.Close()
		return nil, err
	}

	keyBytes := make([]byte, 16)
	if _, err := rand.Read(keyBytes); err != nil {
		conn.Close()
		return nil, err
	}
	key := base64.StdEncoding.EncodeToString(keyBytes)

	reqLine := fmt.Sprintf(
		"GET %s HTTP/1.1\r\nHost: 127.0.0.1:%d\r\nUpgrade: websocket\r\n"+
			"Connection: Upgrade\r\nSec-WebSocket-Key: %s\r\n"+
			"Sec-WebSocket-Version: 13\r\n\r\n",
		path, port, key)
	if _, err := io.WriteString(conn, reqLine); err != nil {
		conn.Close()
		return nil, err
	}

	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("read response: %w", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 101 {
		conn.Close()
		return nil, fmt.Errorf("expected 101 Switching Protocols, got %d", resp.StatusCode)
	}
	h := sha1.New()
	h.Write([]byte(key + wsAcceptGUID))
	wantAccept := base64.StdEncoding.EncodeToString(h.Sum(nil))
	if got := resp.Header.Get("Sec-WebSocket-Accept"); got != wantAccept {
		conn.Close()
		return nil, fmt.Errorf("bad Sec-WebSocket-Accept: got %q, want %q", got, wantAccept)
	}

	return &wsClient{conn: conn, br: br}, nil
}

func (c *wsClient) Close() {
	// Best-effort close frame (no payload) then TCP close.
	_ = c.writeFrame(0x8, nil) // opcode 8 = close
	c.conn.Close()
}

// SendText writes a text frame (masked, since client→server is always
// masked per RFC 6455).
func (c *wsClient) SendText(payload string) error {
	return c.writeFrame(0x1, []byte(payload))
}

func (c *wsClient) writeFrame(opcode byte, payload []byte) error {
	if err := c.conn.SetWriteDeadline(time.Now().Add(2 * time.Second)); err != nil {
		return err
	}
	var header [14]byte
	header[0] = 0x80 | opcode // FIN + opcode
	n := len(payload)
	var hdrLen int
	switch {
	case n < 126:
		header[1] = 0x80 | byte(n) // MASK bit + length
		hdrLen = 2
	case n <= 0xFFFF:
		header[1] = 0x80 | 126
		binary.BigEndian.PutUint16(header[2:4], uint16(n))
		hdrLen = 4
	default:
		header[1] = 0x80 | 127
		binary.BigEndian.PutUint64(header[2:10], uint64(n))
		hdrLen = 10
	}
	var mask [4]byte
	if _, err := rand.Read(mask[:]); err != nil {
		return err
	}
	copy(header[hdrLen:hdrLen+4], mask[:])
	hdrLen += 4

	if _, err := c.conn.Write(header[:hdrLen]); err != nil {
		return err
	}
	if n > 0 {
		masked := make([]byte, n)
		for i := 0; i < n; i++ {
			masked[i] = payload[i] ^ mask[i%4]
		}
		if _, err := c.conn.Write(masked); err != nil {
			return err
		}
	}
	return nil
}

// ReadText reads the next text frame from the server. Control frames
// (ping, close) are handled internally; binary frames return an error.
// Returns io.EOF when the server closes the connection.
func (c *wsClient) ReadText(deadline time.Duration) (string, error) {
	if err := c.conn.SetReadDeadline(time.Now().Add(deadline)); err != nil {
		return "", err
	}
	for {
		var hdr [2]byte
		if _, err := io.ReadFull(c.br, hdr[:]); err != nil {
			return "", err
		}
		fin := hdr[0]&0x80 != 0
		opcode := hdr[0] & 0x0F
		masked := hdr[1]&0x80 != 0
		length := int(hdr[1] & 0x7F)
		if length == 126 {
			var ext [2]byte
			if _, err := io.ReadFull(c.br, ext[:]); err != nil {
				return "", err
			}
			length = int(binary.BigEndian.Uint16(ext[:]))
		} else if length == 127 {
			var ext [8]byte
			if _, err := io.ReadFull(c.br, ext[:]); err != nil {
				return "", err
			}
			length = int(binary.BigEndian.Uint64(ext[:]))
		}
		// Server frames shouldn't be masked per spec; tolerate
		// anyway.
		var mask [4]byte
		if masked {
			if _, err := io.ReadFull(c.br, mask[:]); err != nil {
				return "", err
			}
		}
		payload := make([]byte, length)
		if length > 0 {
			if _, err := io.ReadFull(c.br, payload); err != nil {
				return "", err
			}
			if masked {
				for i := 0; i < length; i++ {
					payload[i] ^= mask[i%4]
				}
			}
		}

		switch opcode {
		case 0x1: // text
			if !fin {
				return "", errors.New("fragmented text frame not supported in test client")
			}
			return string(payload), nil
		case 0x2: // binary
			if !fin {
				return "", errors.New("fragmented binary frame not supported in test client")
			}
			return string(payload), nil // surface as string for assertion
		case 0x8: // close
			return "", io.EOF
		case 0x9: // ping → reply with pong
			if err := c.writeFrame(0xA, payload); err != nil {
				return "", err
			}
		case 0xA: // pong — ignore
		default:
			return "", fmt.Errorf("unknown opcode %d", opcode)
		}
	}
}

// expectNoMessage blocks for up to dur and reports an error if any
// frame arrives. Used to assert that a non-subscriber doesn't receive
// broadcasts.
func (c *wsClient) expectNoMessage(dur time.Duration) error {
	msg, err := c.ReadText(dur)
	if errors.Is(err, io.EOF) {
		return nil
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return nil // expected timeout — no message arrived
	}
	if err != nil {
		return err
	}
	return fmt.Errorf("unexpected message: %q", msg)
}

