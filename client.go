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

type PublishOptions struct {
	ID       string
	MaxRetry int
	RunAt    *time.Time
}

type ClientOptions struct {
	Timeout    time.Duration
	APIKey     string
	Username   string
	Password   string
	HTTPClient *http.Client
}

type Client struct {
	baseURL    string
	httpClient *http.Client
	apiKey     string
	username   string
	password   string
}

type Producer struct {
	client *Client
	queue  string
}

type Consumer struct {
	client *Client
	queue  string
}

func NewClient(baseURL string) *Client {
	return NewClientWithOptions(baseURL, ClientOptions{})
}

func NewClientWithOptions(baseURL string, opts ClientOptions) *Client {
	httpClient := opts.HTTPClient
	if httpClient == nil {
		timeout := opts.Timeout
		if timeout <= 0 {
			timeout = 10 * time.Second
		}
		httpClient = &http.Client{
			Timeout: timeout,
		}
	}
	return &Client{
		baseURL:    strings.TrimRight(baseURL, "/"),
		httpClient: httpClient,
		apiKey:     strings.TrimSpace(opts.APIKey),
		username:   strings.TrimSpace(opts.Username),
		password:   opts.Password,
	}
}

func NewProducer(baseURL string, queue string) (*Producer, error) {
	return NewProducerWithClient(NewClient(baseURL), queue)
}

func NewProducerWithClient(client *Client, queue string) (*Producer, error) {
	if client == nil {
		return nil, errors.New("client is required")
	}
	queue = strings.TrimSpace(queue)
	if queue == "" {
		return nil, errors.New("queue is required")
	}
	return &Producer{
		client: client,
		queue:  queue,
	}, nil
}

func NewConsumer(baseURL string, queue string) (*Consumer, error) {
	return NewConsumerWithClient(NewClient(baseURL), queue)
}

func NewConsumerWithClient(client *Client, queue string) (*Consumer, error) {
	if client == nil {
		return nil, errors.New("client is required")
	}
	queue = strings.TrimSpace(queue)
	if queue == "" {
		return nil, errors.New("queue is required")
	}
	return &Consumer{
		client: client,
		queue:  queue,
	}, nil
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

func (p *Producer) Queue() string {
	return p.queue
}

func (p *Producer) Client() *Client {
	return p.client
}

func (p *Producer) Publish(ctx context.Context, payload any, opts PublishOptions) (Job, error) {
	raw, err := marshalPayload(payload)
	if err != nil {
		return Job{}, err
	}
	return p.client.Enqueue(ctx, EnqueueRequest{
		ID:       opts.ID,
		Queue:    p.queue,
		Payload:  raw,
		MaxRetry: opts.MaxRetry,
		RunAt:    opts.RunAt,
	})
}

func (c *Consumer) Queue() string {
	return c.queue
}

func (c *Consumer) Client() *Client {
	return c.client
}

func (c *Consumer) Receive(ctx context.Context) (Job, error) {
	return c.client.Dequeue(ctx, c.queue)
}

func (c *Consumer) Ack(ctx context.Context, jobID string) error {
	return c.client.Ack(ctx, jobID)
}

func (c *Consumer) AckJob(ctx context.Context, job Job) error {
	return c.client.Ack(ctx, job.ID)
}

func (c *Consumer) Fail(ctx context.Context, jobID string) error {
	return c.client.Fail(ctx, jobID)
}

func (c *Consumer) FailJob(ctx context.Context, job Job) error {
	return c.client.Fail(ctx, job.ID)
}

func marshalPayload(payload any) (json.RawMessage, error) {
	if payload == nil {
		return json.RawMessage(`{}`), nil
	}
	switch v := payload.(type) {
	case json.RawMessage:
		if len(v) == 0 {
			return json.RawMessage(`{}`), nil
		}
		return v, nil
	case []byte:
		if len(v) == 0 {
			return json.RawMessage(`{}`), nil
		}
		return json.RawMessage(v), nil
	case string:
		if strings.TrimSpace(v) == "" {
			return json.RawMessage(`{}`), nil
		}
		return json.RawMessage(v), nil
	default:
		raw, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		return raw, nil
	}
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
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	} else if c.username != "" && strings.TrimSpace(c.password) != "" {
		req.SetBasicAuth(c.username, c.password)
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
