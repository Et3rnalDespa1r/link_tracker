package client

import (
	"context"
	"encoding/json"
	"fmt"
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
	var latest *domain.UpdateDetail
	for _, kind := range []string{"answers", "comments"} {
		endpoint := fmt.Sprintf("%s/questions/%s/%s?site=stackoverflow&order=desc&sort=creation&filter=withbody", c.baseURL, questionID, kind)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, err
		}
		resp, err := c.client.Do(req)
		if err != nil {
			return nil, err
		}
		var result SOAnswerResponse
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			return nil, fmt.Errorf("StackOverflow HTTP %d", resp.StatusCode)
		}
		err = json.NewDecoder(resp.Body).Decode(&result)
		resp.Body.Close()
		if err != nil {
			return nil, err
		}
		for _, item := range result.Items {
			createdAt := time.Unix(item.CreationDate, 0)
			if createdAt.After(lastCheck) && (latest == nil || createdAt.After(latest.CreatedAt)) {
				eventType := "StackOverflow Answer"
				if kind == "comments" {
					eventType = "StackOverflow Comment"
				}
				latest = &domain.UpdateDetail{Type: eventType, Title: "StackOverflow question " + questionID, Author: item.Owner.DisplayName, CreatedAt: createdAt, Preview: truncateText(item.Body, 200)}
			}
		}
	}
	if latest != nil {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s/questions/%s?site=stackoverflow", c.baseURL, questionID), nil)
		if err != nil {
			return nil, err
		}
		resp, err := c.client.Do(req)
		if err != nil {
			return nil, err
		}
		var question struct {
			Items []struct {
				Title string `json:"title"`
			} `json:"items"`
		}
		if resp.StatusCode != 200 {
			resp.Body.Close()
			return nil, fmt.Errorf("StackOverflow HTTP %d", resp.StatusCode)
		}
		err = json.NewDecoder(resp.Body).Decode(&question)
		resp.Body.Close()
		if err != nil {
			return nil, err
		}
		if len(question.Items) > 0 && question.Items[0].Title != "" {
			latest.Title = question.Items[0].Title
		}
	}
	return latest, nil
}
