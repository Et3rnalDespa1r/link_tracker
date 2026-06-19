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

type SOAnswerResponse struct {
	Items []struct {
		AnswerID     int64  `json:"answer_id"`
		CreationDate int64  `json:"creation_date"`
		Body         string `json:"body"`
		Owner        struct {
			DisplayName string `json:"display_name"`
		} `json:"owner"`
	} `json:"items"`
}

type StackOverflowClient struct {
	baseURL string
	client  *ResilientHTTPClient
}

func NewStackOverflowClient(baseURL string, cfg *config.Config) *StackOverflowClient {
	return &StackOverflowClient{
		baseURL: baseURL,
		client:  NewResilientHTTPClient(cfg, "stackoverflow"),
	}
}

func (c *StackOverflowClient) FetchLatestAnswer(ctx context.Context, questionID string, lastCheck time.Time) (*domain.UpdateDetail, error) {
	url := fmt.Sprintf("%s/questions/%s/answers?site=stackoverflow&order=desc&sort=creation&filter=!6WPIomnb-D05L", c.baseURL, questionID)

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
		return nil, fmt.Errorf("unexpected status code: %d, body: %s", resp.StatusCode, string(bodyBytes))
	}

	var result SOAnswerResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode error: %w", err)
	}

	for _, item := range result.Items {
		createdAt := time.Unix(item.CreationDate, 0)

		if createdAt.After(lastCheck) {
			return &domain.UpdateDetail{
				Type:      "StackOverflow Answer",
				Title:     "Новый ответ в StackOverflow",
				Author:    item.Owner.DisplayName,
				CreatedAt: createdAt,
				Preview:   truncateText(item.Body, 200),
			}, nil
		}
	}
	return nil, nil
}
