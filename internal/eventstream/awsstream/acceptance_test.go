package awsstream

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
)

// TestAcceptance_KiroLikeStream simulates a Kiro `generateAssistantResponse`
// stream of three frames:
//   1. event-type=initial     payload={"role":"assistant"}
//   2. event-type=content     payload={"text":"Hello "}
//   3. event-type=content     payload={"text":"world!"}
// The test verifies all frames decode and event-type ordering is preserved.
func TestAcceptance_KiroLikeStream(t *testing.T) {
	frames := [][]byte{
		makeFrame(buildStringHeader(":event-type", "initial"), []byte(`{"role":"assistant"}`)),
		makeFrame(buildStringHeader(":event-type", "content"), []byte(`{"text":"Hello "}`)),
		makeFrame(buildStringHeader(":event-type", "content"), []byte(`{"text":"world!"}`)),
	}
	wire := bytes.Join(frames, nil)

	d := NewDecoder(bytes.NewReader(wire))
	var (
		eventTypes []string
		texts      []string
	)
	for {
		f, err := d.ReadFrame()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("ReadFrame: %v", err)
		}
		et, _ := f.StringHeader(":event-type")
		eventTypes = append(eventTypes, et)
		if et == "content" {
			// crude: extract "text" via substring; M2b will use proper JSON.
			s := string(f.Payload)
			start := strings.Index(s, `"text":"`) + len(`"text":"`)
			end := strings.LastIndex(s, `"`)
			texts = append(texts, s[start:end])
		}
	}

	if len(eventTypes) != 3 {
		t.Fatalf("expected 3 events, got %d: %v", len(eventTypes), eventTypes)
	}
	if eventTypes[0] != "initial" || eventTypes[1] != "content" || eventTypes[2] != "content" {
		t.Errorf("event order = %v; want [initial content content]", eventTypes)
	}
	joined := strings.Join(texts, "")
	if joined != "Hello world!" {
		t.Errorf("joined text = %q; want %q", joined, "Hello world!")
	}
}

// TestAcceptance_PartialReadHandling verifies the decoder works against a
// io.Reader that returns data in small chunks (simulating network slow read).
func TestAcceptance_PartialReadHandling(t *testing.T) {
	wire := makeFrame(buildStringHeader(":event-type", "x"), []byte(`{"k":"v"}`))
	d := NewDecoder(&choppyReader{src: wire, max: 3})
	f, err := d.ReadFrame()
	if err != nil {
		t.Fatalf("ReadFrame on choppy reader: %v", err)
	}
	if et, _ := f.StringHeader(":event-type"); et != "x" {
		t.Errorf("event-type = %q; want x", et)
	}
}

// choppyReader returns at most max bytes per Read call.
type choppyReader struct {
	src []byte
	max int
}

func (c *choppyReader) Read(p []byte) (int, error) {
	if len(c.src) == 0 {
		return 0, io.EOF
	}
	n := len(p)
	if n > c.max {
		n = c.max
	}
	if n > len(c.src) {
		n = len(c.src)
	}
	copy(p, c.src[:n])
	c.src = c.src[n:]
	return n, nil
}
