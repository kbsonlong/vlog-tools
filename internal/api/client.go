package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/hashicorp/go-retryablehttp"
	"go.uber.org/zap"
)

type Client struct {
	httpClient *retryablehttp.Client
	baseURL    string
	logger     *zap.Logger
}

func NewClient(baseURL string, logger *zap.Logger) *Client {
	retryClient := retryablehttp.NewClient()
	retryClient.RetryMax = 3
	retryClient.Logger = nil // Disable default logger

	return &Client{
		httpClient: retryClient,
		baseURL:    strings.TrimRight(baseURL, "/"),
		logger:     logger,
	}
}

func (c *Client) CreatePartitionSnapshot(ctx context.Context, partition string, authKey string) ([]string, error) {
	u, err := url.Parse(c.baseURL + "/internal/partition/snapshot/create")
	if err != nil {
		return nil, err
	}
	q := u.Query()
	q.Set("partition_prefix", partition)
	if authKey != "" {
		q.Set("authKey", authKey)
	}
	u.RawQuery = q.Encode()

	req, err := retryablehttp.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("create partition snapshot: status=%d body=%s", resp.StatusCode, string(body))
	}

	var paths []string
	if err := json.Unmarshal(body, &paths); err != nil {
		return nil, fmt.Errorf("decode partition snapshot response %q: %w", string(body), err)
	}
	if len(paths) == 0 {
		return nil, fmt.Errorf("create partition snapshot: empty snapshot list for partition %s", partition)
	}
	return paths, nil
}

func (c *Client) DeletePartitionSnapshot(ctx context.Context, snapshotPath string, authKey string) error {
	u, err := url.Parse(c.baseURL + "/internal/partition/snapshot/delete")
	if err != nil {
		return err
	}
	q := u.Query()
	q.Set("path", snapshotPath)
	if authKey != "" {
		q.Set("authKey", authKey)
	}
	u.RawQuery = q.Encode()

	req, err := retryablehttp.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("delete partition snapshot: status=%d body=%s", resp.StatusCode, string(body))
	}
	return nil
}

func (c *Client) ReloadPartition(ctx context.Context, partition string) error {
	url := fmt.Sprintf("%s/internal/force_flush", c.baseURL)

	req, err := retryablehttp.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to reload partition: status=%d body=%s", resp.StatusCode, string(body))
	}

	return nil
}

type QueryRequest struct {
	TimeRange TimeRange
	Query     string
}

type TimeRange struct {
	Start time.Time
	End   time.Time
}

type QueryResult struct {
	Records []interface{}
}

func (c *Client) Query(ctx context.Context, query QueryRequest) (*QueryResult, error) {
	// Not fully implemented for now, placeholder for Query engine
	return &QueryResult{}, nil
}
