package cache

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/rueidis"
)

type ResponseCache struct {
	client rueidis.Client
	ttl    time.Duration
}

func NewResponseCache(client rueidis.Client, ttl time.Duration) *ResponseCache {
	return &ResponseCache{
		client: client,
		ttl:    ttl,
	}
}

func (r *ResponseCache) GetList(ctx context.Context, chatID int64) ([]byte, error) {
	if r.client == nil {
		return nil, nil
	}

	key := fmt.Sprintf("links:chat_id:%d", chatID)

	cmd := r.client.B().Get().Key(key).Cache()
	resp := r.client.DoCache(ctx, cmd, r.ttl)

	if err := resp.Error(); err != nil {
		if rueidis.IsRedisNil(err) {
			return nil, nil
		}
		return nil, err
	}

	str, err := resp.ToString()
	if err != nil {
		return nil, err
	}
	return []byte(str), nil
}

func (r *ResponseCache) SetList(ctx context.Context, chatID int64, data []byte) error {
	if r.client == nil {
		return nil
	}

	key := fmt.Sprintf("links:chat_id:%d", chatID)
	cmd := r.client.B().Set().Key(key).Value(string(data)).Ex(r.ttl).Build()
	return r.client.Do(ctx, cmd).Error()
}

func (r *ResponseCache) Invalidate(ctx context.Context, chatID int64) error {
	if r.client == nil {
		return nil
	}

	key := fmt.Sprintf("links:chat_id:%d", chatID)
	cmd := r.client.B().Del().Key(key).Build()
	return r.client.Do(ctx, cmd).Error()
}
