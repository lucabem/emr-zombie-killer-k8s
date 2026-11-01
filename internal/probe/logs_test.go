package probe

import (
	"testing"
	"time"
)

func TestParseLastTimestamp(t *testing.T) {
	got, ok, err := ParseLastTimestamp("2026-01-01T10:00:00.123456789Z 24/01/01 INFO SparkContext: hello\n")
	if err != nil || !ok {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
	if want := time.Date(2026, 1, 1, 10, 0, 0, 123456789, time.UTC); !got.Equal(want) {
		t.Fatalf("got %v want %v", got, want)
	}
}

func TestParseEmpty(t *testing.T) {
	if _, ok, err := ParseLastTimestamp("  \n"); ok || err != nil {
		t.Fatalf("empty output: ok=%v err=%v", ok, err)
	}
}

func TestParseGarbage(t *testing.T) {
	if _, _, err := ParseLastTimestamp("not-a-timestamp message"); err == nil {
		t.Fatal("expected error")
	}
}
