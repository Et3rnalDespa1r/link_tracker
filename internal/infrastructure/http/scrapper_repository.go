package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/domain"
)

type ScrapperRepository struct {
	base   string
	client *http.Client
}

func NewScrapperRepository(base string, timeout time.Duration) *ScrapperRepository {
	return &ScrapperRepository{base, &http.Client{Timeout: timeout}}
}
func (r *ScrapperRepository) request(ctx context.Context, method, path string, id int64, body any, output any) error {
	data, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, method, r.base+path, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Tg-Chat-Id", strconv.FormatInt(id, 10))
	req.Header.Set("Content-Type", "application/json")
	resp, err := r.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("scrapper returned HTTP %d", resp.StatusCode)
	}
	if output != nil {
		return json.NewDecoder(resp.Body).Decode(output)
	}
	return nil
}
func (r *ScrapperRepository) RegisterChat(ctx context.Context, id int64) error {
	return r.request(ctx, "POST", "/tg-chat/"+strconv.FormatInt(id, 10), id, nil, nil)
}
func (r *ScrapperRepository) AddLink(ctx context.Context, id int64, link domain.Link) error {
	return r.request(ctx, "POST", "/links", id, map[string]any{"link": link.URL, "tags": link.Tags}, nil)
}
func (r *ScrapperRepository) RemoveLink(ctx context.Context, id int64, link string) error {
	return r.request(ctx, "DELETE", "/links", id, map[string]string{"link": link}, nil)
}
func (r *ScrapperRepository) GetLinks(ctx context.Context, id int64) ([]domain.Link, error) {
	var response struct {
		Links []struct {
			ID   int64    `json:"id"`
			URL  string   `json:"url"`
			Tags []string `json:"tags"`
		} `json:"links"`
	}
	if err := r.request(ctx, "GET", "/links", id, nil, &response); err != nil {
		return nil, err
	}
	links := make([]domain.Link, 0, len(response.Links))
	for _, link := range response.Links {
		links = append(links, domain.Link{ID: link.ID, URL: link.URL, Tags: link.Tags})
	}
	return links, nil
}
func (r *ScrapperRepository) HasLink(ctx context.Context, id int64, url string) (bool, error) {
	links, err := r.GetLinks(ctx, id)
	if err != nil {
		return false, err
	}
	for _, link := range links {
		if link.URL == url {
			return true, nil
		}
	}
	return false, nil
}
