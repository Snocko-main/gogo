// loadgen — single binary that drives a uWS-or-gogo server over
// WebSocket, then prints throughput + memory.
//
// Phases:
//   1. Snapshot /stat → baseline RSS
//   2. Open --conns WebSocket connections, each subscribed via the
//      server's Open handler. Stream-read frames in a goroutine,
//      bumping a per-connection counter. Wait for all to attach.
//   3. Snapshot /stat → idle-with-conns RSS
//   4. Drive --duration of /publish or /publishbatch with batches
//      of size --batch from a single HTTP client.
//   5. Snapshot /stat → during/after workload RSS
//   6. Print summary table.

package main

import (
	"bufio"
	"crypto/rand"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type stat struct {
	RSS      int64 `json:"rss"`
	HeapUsed int64 `json:"heapUsed"`
	Conns    int64 `json:"conns"`
}

func fetchStat(base string) (stat, error) {
	resp, err := http.Get(base + "/stat")
	if err != nil {
		return stat{}, err
	}
	defer resp.Body.Close()
	var s stat
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
		return stat{}, err
	}
	return s, nil
}

func closeHTTPResponse(resp *http.Response) {
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
}

const wsAcceptGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

func dialWS(addr, path string) (net.Conn, *bufio.Reader, error) {
	conn, err := net.DialTimeout("tcp", addr, 10*time.Second)
	if err != nil {
		return nil, nil, err
	}
	conn.SetDeadline(time.Now().Add(15 * time.Second))
	keyBytes := make([]byte, 16)
	rand.Read(keyBytes)
	key := base64.StdEncoding.EncodeToString(keyBytes)
	req := fmt.Sprintf(
		"GET %s HTTP/1.1\r\nHost: %s\r\nUpgrade: websocket\r\n"+
			"Connection: Upgrade\r\nSec-WebSocket-Key: %s\r\n"+
			"Sec-WebSocket-Version: 13\r\n\r\n",
		path, addr, key)
	if _, err := io.WriteString(conn, req); err != nil {
		conn.Close()
		return nil, nil, err
	}
	br := bufio.NewReaderSize(conn, 4096)
	// Parse handshake headers MANUALLY — http.ReadResponse +
	// resp.Body.Close() races against the server starting to send
	// WS frames immediately after the 101 response and can hand the
	// underlying TCP socket to the body-discard goroutine.
	statusLine, err := br.ReadString('\n')
	if err != nil {
		conn.Close()
		return nil, nil, err
	}
	if !strings.HasPrefix(statusLine, "HTTP/1.1 101") {
		conn.Close()
		return nil, nil, fmt.Errorf("expected 101, got %q", statusLine)
	}
	var accept string
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			conn.Close()
			return nil, nil, err
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			break
		}
		if idx := strings.IndexByte(line, ':'); idx > 0 {
			name := strings.TrimSpace(line[:idx])
			value := strings.TrimSpace(line[idx+1:])
			if strings.EqualFold(name, "Sec-WebSocket-Accept") {
				accept = value
			}
		}
	}
	h := sha1.New()
	h.Write([]byte(key + wsAcceptGUID))
	if accept != base64.StdEncoding.EncodeToString(h.Sum(nil)) {
		conn.Close()
		return nil, nil, errors.New("bad Sec-WebSocket-Accept")
	}
	conn.SetDeadline(time.Time{})
	return conn, br, nil
}

// readFrames pulls every text/binary frame off conn and bumps
// counter. Returns when the connection errors out (server close,
// network reset, EOF, etc).
func readFrames(br *bufio.Reader, counter *atomic.Int64) {
	for {
		var hdr [2]byte
		if _, err := io.ReadFull(br, hdr[:]); err != nil {
			return
		}
		opcode := hdr[0] & 0x0F
		masked := hdr[1]&0x80 != 0
		length := int(hdr[1] & 0x7F)
		if length == 126 {
			var ext [2]byte
			if _, err := io.ReadFull(br, ext[:]); err != nil {
				return
			}
			length = int(binary.BigEndian.Uint16(ext[:]))
		} else if length == 127 {
			var ext [8]byte
			if _, err := io.ReadFull(br, ext[:]); err != nil {
				return
			}
			length = int(binary.BigEndian.Uint64(ext[:]))
		}
		var mask [4]byte
		if masked {
			if _, err := io.ReadFull(br, mask[:]); err != nil {
				return
			}
		}
		if length > 0 {
			payload := make([]byte, length)
			if _, err := io.ReadFull(br, payload); err != nil {
				return
			}
		}
		if opcode == 0x1 || opcode == 0x2 {
			counter.Add(1)
		}
		if opcode == 0x8 {
			return
		}
	}
}

func main() {
	var (
		target   = flag.String("target", "127.0.0.1:7000", "host:port of server")
		conns    = flag.Int("conns", 1000, "concurrent WebSocket connections")
		duration = flag.Duration("duration", 5*time.Second, "publish phase duration")
		batch    = flag.Int("batch", 1, "messages per /publish or /publishbatch call")
		mode     = flag.String("mode", "publish", "publish | publishbatch | idle")
		label    = flag.String("label", "server", "label for the report")
		rate     = flag.Int("rate", 0, "publish calls per second (0 = full speed)")
	)
	flag.Parse()

	base := "http://" + *target

	// Phase 1 — baseline
	baseStat, err := fetchStat(base)
	if err != nil {
		fmt.Fprintln(os.Stderr, "baseline /stat:", err)
		os.Exit(1)
	}

	// Phase 2 — open conns
	var counter atomic.Int64
	conns_ := make([]net.Conn, 0, *conns)

	connectStart := time.Now()
	var failed atomic.Int64
	// Sequential dial with a 1 ms space — keeps the server's HTTP
	// layer from getting buried under a burst of upgrades and gives
	// each Open callback a tick to settle. Total connect time at
	// 1000 conns is ~1.5 s on this VM, which is fine for a once-per
	// run setup phase. Sequential also means no mutex on conns_.
	for i := 0; i < *conns; i++ {
		c, br, err := dialWS(*target, "/ws")
		if err != nil {
			failed.Add(1)
			continue
		}
		conns_ = append(conns_, c)
		go readFrames(br, &counter)
		time.Sleep(1 * time.Millisecond)
	}
	connectElapsed := time.Since(connectStart)

	established := len(conns_)
	// Wait until the server's own conn counter stabilizes (Open
	// handlers run async on the loop after the TCP handshake
	// returns to the client). Poll /stat for up to 3s.
	var idleStat stat
	for i := 0; i < 30; i++ {
		s, err := fetchStat(base)
		if err == nil {
			idleStat = s
			if int(s.Conns) >= established || i == 29 {
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Phase 3 — drive publish
	var publishPath string
	switch *mode {
	case "publish":
		publishPath = "/publish"
	case "publishbatch":
		publishPath = "/publishbatch"
	case "idle":
		publishPath = ""
	default:
		fmt.Fprintln(os.Stderr, "unknown mode:", *mode)
		os.Exit(1)
	}

	httpClient := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        16,
			MaxIdleConnsPerHost: 16,
			MaxConnsPerHost:     16,
		},
		Timeout: 30 * time.Second,
	}
	publishURL := fmt.Sprintf("%s%s?n=%d", base, publishPath, *batch)

	startCount := counter.Load()
	workStart := time.Now()
	deadline := workStart.Add(*duration)

	var publishCalls atomic.Int64
	if *mode == "idle" {
		// No publish — just sleep for the duration so we can capture
		// idle RSS over time. Useful for connection-count tests.
		time.Sleep(*duration)
	} else if *rate > 0 {
		ticker := time.NewTicker(time.Second / time.Duration(*rate))
		defer ticker.Stop()
		for time.Now().Before(deadline) {
			<-ticker.C
			resp, err := httpClient.Get(publishURL)
			if err == nil {
				closeHTTPResponse(resp)
				publishCalls.Add(1)
			}
		}
	} else {
		// Saturate — 4 workers pumping concurrently.
		var wg sync.WaitGroup
		for w := 0; w < 4; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for time.Now().Before(deadline) {
					resp, err := httpClient.Get(publishURL)
					if err == nil {
						closeHTTPResponse(resp)
						publishCalls.Add(1)
					}
				}
			}()
		}
		wg.Wait()
	}

	workElapsed := time.Since(workStart)
	// Let in-flight publishes drain.
	time.Sleep(500 * time.Millisecond)
	endCount := counter.Load()
	loadStat, _ := fetchStat(base)

	// Close all conns gracefully so the server's RSS reflects the
	// post-drain steady state on the next snapshot.
	for _, c := range conns_ {
		c.Close()
	}
	time.Sleep(500 * time.Millisecond)
	postStat, _ := fetchStat(base)

	delivered := endCount - startCount
	msgsPerSec := float64(delivered) / workElapsed.Seconds()
	publishedMsgs := publishCalls.Load() * int64(*batch)
	publishesPerSec := float64(publishCalls.Load()) / workElapsed.Seconds()

	fmt.Println("=== ", *label, " ===")
	fmt.Printf("target=%s  mode=%s  batch=%d  conns_requested=%d  conns_established=%d  failed=%d\n",
		*target, *mode, *batch, *conns, established, failed.Load())
	fmt.Printf("connect_time=%.2fs  workload=%.2fs\n", connectElapsed.Seconds(), workElapsed.Seconds())
	fmt.Printf("publishes_issued=%d  msgs_published=%d  publishes/sec=%.0f\n",
		publishCalls.Load(), publishedMsgs, publishesPerSec)
	fmt.Printf("msgs_delivered=%d  msgs_delivered/sec=%.0f  delivered/published=%.2f\n",
		delivered, msgsPerSec,
		func() float64 {
			if publishedMsgs == 0 {
				return 0
			}
			return float64(delivered) / float64(publishedMsgs*int64(established))
		}())
	fmt.Printf("RSS(MB):  baseline=%.1f  idle(N=%d)=%.1f  load=%.1f  post-drain=%.1f\n",
		float64(baseStat.RSS)/1024/1024,
		idleStat.Conns,
		float64(idleStat.RSS)/1024/1024,
		float64(loadStat.RSS)/1024/1024,
		float64(postStat.RSS)/1024/1024)
	if idleStat.Conns > 0 {
		perConn := float64(idleStat.RSS-baseStat.RSS) / float64(idleStat.Conns)
		fmt.Printf("memory/conn (idle): %.0f bytes\n", perConn)
	}
}
