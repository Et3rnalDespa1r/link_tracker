package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/domain"
)

type BotClient struct {
	baseURL string
	client  *http.Client
}

func NewBotClient(baseURL string) *BotClient {
	return &BotClient{
		baseURL: baseURL,
		client:  &http.Client{Timeout: 10 * time.Second},
	}
}

func (c *BotClient) SendUpdate(ctx context.Context, update domain.LinkUpdate) error {
	body, err := json.Marshal(update)
	if err != nil {
		return fmt.Errorf("marshal error: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/updates", bytes.NewBuffer(body))
	if err != nil {
		return fmt.Errorf("request error: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("do error: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("unexpected status: %d, body: %s", resp.StatusCode, string(bodyBytes))
	}

	return nil
}
