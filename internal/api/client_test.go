package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go.uber.org/zap"
)

func TestCreatePartitionSnapshot(t *testing.T) {
	var gotPath string
	var gotPartition string
	var gotAuthKey string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotPartition = r.URL.Query().Get("partition_prefix")
		gotAuthKey = r.URL.Query().Get("authKey")
		_, _ = w.Write([]byte(`["/data/vlstorage/snapshots/abc/20260621"]`))
	}))
	defer srv.Close()

	c := NewClient(srv.URL+"/", zap.NewNop())
	paths, err := c.CreatePartitionSnapshot(context.Background(), "20260621", "secret")
	if err != nil {
		t.Fatalf("CreatePartitionSnapshot() error = %v", err)
	}
	if gotPath != "/internal/partition/snapshot/create" {
		t.Fatalf("path = %q, want snapshot create endpoint", gotPath)
	}
	if gotPartition != "20260621" {
		t.Fatalf("partition_prefix = %q, want 20260621", gotPartition)
	}
	if gotAuthKey != "secret" {
		t.Fatalf("authKey = %q, want secret", gotAuthKey)
	}
	if len(paths) != 1 || paths[0] != "/data/vlstorage/snapshots/abc/20260621" {
		t.Fatalf("paths = %#v", paths)
	}
}

func TestCreatePartitionSnapshotFallsBackToLegacyNameArg(t *testing.T) {
	var firstQuery string
	var secondQuery string
	callCount := 0

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		if callCount == 1 {
			firstQuery = r.URL.RawQuery
			http.Error(w, `cannot create snapshot from partition "", because it is missing`, http.StatusBadRequest)
			return
		}
		secondQuery = r.URL.RawQuery
		_, _ = w.Write([]byte(`["/data/vlstorage/snapshots/abc/20260621"]`))
	}))
	defer srv.Close()

	c := NewClient(srv.URL, zap.NewNop())
	paths, err := c.CreatePartitionSnapshot(context.Background(), "20260621", "")
	if err != nil {
		t.Fatalf("CreatePartitionSnapshot() error = %v", err)
	}
	if !strings.Contains(firstQuery, "partition_prefix=20260621") {
		t.Fatalf("first query = %q", firstQuery)
	}
	if !strings.Contains(secondQuery, "name=20260621") {
		t.Fatalf("second query = %q", secondQuery)
	}
	if len(paths) != 1 {
		t.Fatalf("paths = %#v", paths)
	}
}

func TestDeletePartitionSnapshot(t *testing.T) {
	var gotPath string
	var gotSnapshotPath string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotSnapshotPath = r.URL.Query().Get("path")
	}))
	defer srv.Close()

	c := NewClient(srv.URL, zap.NewNop())
	err := c.DeletePartitionSnapshot(context.Background(), "/data/vlstorage/snapshots/abc/20260621", "")
	if err != nil {
		t.Fatalf("DeletePartitionSnapshot() error = %v", err)
	}
	if gotPath != "/internal/partition/snapshot/delete" {
		t.Fatalf("path = %q, want snapshot delete endpoint", gotPath)
	}
	if gotSnapshotPath != "/data/vlstorage/snapshots/abc/20260621" {
		t.Fatalf("snapshot path = %q", gotSnapshotPath)
	}
}
