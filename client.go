package sdk

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

var (
	ErrQueueEmpty = errors.New("queue empty")
	ErrNotFound   = errors.New("not found")
)

type JobStatus string

const (
	StatusPending    JobStatus = "pending"
	StatusProcessing JobStatus = "processing"
	StatusDone       JobStatus = "done"
	StatusFailed     JobStatus = "failed"
)

type Job struct {
	ID        string          `json:"id"`
	Queue     string          `json:"queue"`
	Payload   json.RawMessage `json:"payload"`
	Status    JobStatus       `json:"status"`
	LastError string          `json:"last_error,omitempty"`
	Attempts  int             `json:"attempts"`
	MaxRetry  int             `json:"max_retry"`
	RunAt     time.Time       `json:"run_at"`
	LockedAt  time.Time       `json:"locked_at"`
	CreatedAt time.Time       `json:"created_at"`
}

type EnqueueRequest struct {
	ID       string          `json:"id,omitempty"`
	Queue    string          `json:"queue"`
	Payload  json.RawMessage `json:"payload,omitempty"`
	MaxRetry int             `json:"max_retry,omitempty"`
	RunAt    *time.Time      `json:"run_at,omitempty"`
}

type Client struct {
	baseURL    string
	httpClient *http.Client
}

func NewClient(baseURL string) *Client {
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func (c *Client) Enqueue(ctx context.Context, req EnqueueRequest) (Job, error) {
	if strings.TrimSpace(req.Queue) == "" {
		return Job{}, errors.New("queue is required")
	}
	var out Job
	if err := c.doJSON(ctx, http.MethodPost, "/enqueue", req, http.StatusCreated, &out); err != nil {
		return Job{}, err
	}
	return out, nil
}

func (c *Client) Dequeue(ctx context.Context, queue string) (Job, error) {
	if strings.TrimSpace(queue) == "" {
		return Job{}, errors.New("queue is required")
	}
	endpoint := "/dequeue?queue=" + url.QueryEscape(queue)
	var out Job
	err := c.doJSON(ctx, http.MethodGet, endpoint, nil, http.StatusOK, &out)
	if errors.Is(err, ErrQueueEmpty) {
		return Job{}, ErrQueueEmpty
	}
	if err != nil {
		return Job{}, err
	}
	return out, nil
}

func (c *Client) Ack(ctx context.Context, id string) error {
	if strings.TrimSpace(id) == "" {
		return errors.New("id is required")
	}
	return c.doJSON(ctx, http.MethodPost, "/ack", map[string]string{"id": id}, http.StatusOK, nil)
}

func (c *Client) Fail(ctx context.Context, id string) error {
	if strings.TrimSpace(id) == "" {
		return errors.New("id is required")
	}
	return c.doJSON(ctx, http.MethodPost, "/fail", map[string]string{"id": id}, http.StatusOK, nil)
}

func (c *Client) doJSON(ctx context.Context, method string, endpoint string, in any, expectedStatus int, out any) error {
	var body io.Reader
	if in != nil {
		raw, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(raw)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+endpoint, body)
	if err != nil {
		return err
	}
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	res, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode == http.StatusNoContent {
		return ErrQueueEmpty
	}
	if res.StatusCode == http.StatusNotFound {
		return ErrNotFound
	}
	if res.StatusCode != expectedStatus {
		raw, _ := io.ReadAll(res.Body)
		var payload map[string]any
		if json.Unmarshal(raw, &payload) == nil {
			if msg, ok := payload["error"].(string); ok && strings.TrimSpace(msg) != "" {
				return errors.New(msg)
			}
		}
		if len(raw) > 0 {
			return errors.New(string(raw))
		}
		return errors.New(res.Status)
	}

	if out == nil {
		return nil
	}
	return json.NewDecoder(res.Body).Decode(out)
}
