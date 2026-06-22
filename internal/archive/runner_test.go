package archive

import (
	"testing"
	"time"
)

func TestPartitionForTimeUsesConfiguredTimezone(t *testing.T) {
	shanghai := time.FixedZone("Asia/Shanghai", 8*60*60)
	now := time.Date(2026, 6, 22, 1, 0, 0, 0, shanghai)

	got := partitionForTime(now, 1, time.UTC)
	if got != "20260620" {
		t.Fatalf("UTC partition = %s, want 20260620", got)
	}

	got = partitionForTime(now, 1, shanghai)
	if got != "20260621" {
		t.Fatalf("Shanghai partition = %s, want 20260621", got)
	}
}
