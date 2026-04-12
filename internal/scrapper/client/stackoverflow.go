package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type StackOverflowItem struct {
	QuestionID       int64 `json:"question_id"`
	LastActivityDate int64 `json:"last_activity_date"`
}

type StackOverflowResponse struct {
	Items []StackOverflowItem `json:"items"`
}

type StackOverflowClient struct {
	baseURL string
	client  *http.Client
}

func NewStackOverflowClient(baseURL string) *StackOverflowClient {
	return &StackOverflowClient{
		baseURL: baseURL,
		client:  &http.Client{Timeout: 10 * time.Second},
	}
}

func (c *StackOverflowClient) FetchQuestion(ctx context.Context, id int64) (*StackOverflowResponse, error) {
	url := fmt.Sprintf("%s/questions/%d?site=stackoverflow", c.baseURL, id)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("request create error: %w", err)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request do error: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode, string(bodyBytes))
	}

	var result StackOverflowResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode error: %w", err)
	}

	return &result, nil
}
