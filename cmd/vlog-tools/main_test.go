package main

import (
	"reflect"
	"testing"
	"time"
)

func TestParsePartitionDate(t *testing.T) {
	tests := []struct {
		input string
		want  time.Time
	}{
		{input: "20260501", want: time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)},
		{input: "2026-05-01", want: time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)},
	}

	for _, tt := range tests {
		got, err := parsePartitionDate(tt.input)
		if err != nil {
			t.Fatalf("parsePartitionDate(%q) error = %v", tt.input, err)
		}
		if !got.Equal(tt.want) {
			t.Fatalf("parsePartitionDate(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}

func TestParsePartitionDateRejectsInvalidInput(t *testing.T) {
	if _, err := parsePartitionDate("2026/05/01"); err == nil {
		t.Fatal("parsePartitionDate() error = nil, want error")
	}
}

func TestPartitionsBetween(t *testing.T) {
	start := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 5, 3, 0, 0, 0, 0, time.UTC)

	got := partitionsBetween(start, end)
	want := []string{"20260501", "20260502", "20260503"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("partitionsBetween() = %#v, want %#v", got, want)
	}
}
