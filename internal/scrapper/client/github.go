package client

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type GithubResponse struct {
	FullName  string    `json:"full_name"`
	UpdatedAt time.Time `json:"updated_at"`
	PushedAt  time.Time `json:"pushed_at"`
}

type GithubClient struct {
	baseURL string
	client  *http.Client
}

func NewGithubClient(baseURL string) *GithubClient {
	return &GithubClient{
		baseURL: baseURL,
		client:  &http.Client{Timeout: 10 * time.Second},
	}
}

func (c *GithubClient) FetchRepository(ctx context.Context, owner, repo string) (*GithubResponse, error) {
	url := fmt.Sprintf("%s/repos/%s/%s", c.baseURL, owner, repo)
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
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var result GithubResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode error: %w", err)
	}

	return &result, nil
}
