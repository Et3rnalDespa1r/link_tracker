package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/config"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/domain"
)

type GithubEvent struct {
	Type      string    `json:"type"`
	CreatedAt time.Time `json:"created_at"`
	Actor     struct {
		Login string `json:"login"`
	} `json:"actor"`
	Payload struct {
		Action string `json:"action"`
		Issue  *struct {
			Title string `json:"title"`
			Body  string `json:"body"`
		} `json:"issue"`
		PullRequest *struct {
			Title string `json:"title"`
			Body  string `json:"body"`
		} `json:"pull_request"`
	} `json:"payload"`
}

type GithubClient struct {
	baseURL string
	client  *ResilientHTTPClient
	token   string
}

func NewGithubClient(baseURL string, token string, cfg *config.Config) *GithubClient {
	return &GithubClient{
		baseURL: baseURL,
		client:  NewResilientHTTPClient(cfg, "github"),
		token:   token,
	}
}

func (c *GithubClient) FetchLatestEvent(ctx context.Context, owner, repo string, lastCheck time.Time) (*domain.UpdateDetail, error) {
	url := fmt.Sprintf("%s/repos/%s/%s/events", c.baseURL, owner, repo)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("request create error: %w", err)
	}

	req.Header.Set("Accept", "application/vnd.github.v3+json")
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request do error: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status code: %d, body: %s", resp.StatusCode, string(bodyBytes))
	}

	var events []GithubEvent
	if err := json.NewDecoder(resp.Body).Decode(&events); err != nil {
		return nil, fmt.Errorf("decode error: %w", err)
	}

	for _, event := range events {
		if !event.CreatedAt.After(lastCheck) {
			continue
		}

		if event.Type == "PushEvent" {
			return &domain.UpdateDetail{
				Type:      "GitHub Push",
				Title:     "Новый коммит в репозиторий",
				Author:    event.Actor.Login,
				CreatedAt: event.CreatedAt,
				Preview:   fmt.Sprintf("Пользователь %s запушил изменения", event.Actor.Login),
			}, nil
		}

		if event.Type == "IssuesEvent" && event.Payload.Action == "opened" {
			return &domain.UpdateDetail{
				Type:      "GitHub Issue",
				Title:     event.Payload.Issue.Title,
				Author:    event.Actor.Login,
				CreatedAt: event.CreatedAt,
				Preview:   truncateText(event.Payload.Issue.Body, 200),
			}, nil
		}

		if event.Type == "PullRequestEvent" && event.Payload.PullRequest != nil {
			return &domain.UpdateDetail{
				Type:      "GitHub PR",
				Title:     event.Payload.PullRequest.Title,
				Author:    event.Actor.Login,
				CreatedAt: event.CreatedAt,
				Preview:   truncateText(event.Payload.PullRequest.Body, 200),
			}, nil
		}
	}

	return nil, nil
}

func truncateText(text string, limit int) string {
	runes := []rune(text)
	if len(runes) <= limit {
		return text
	}
	return string(runes[:limit]) + "..."
}
