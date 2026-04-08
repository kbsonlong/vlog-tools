package api

import (
	"context"
	"fmt"
	"io"
	"net/http"
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
		baseURL:    baseURL,
		logger:     logger,
	}
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
