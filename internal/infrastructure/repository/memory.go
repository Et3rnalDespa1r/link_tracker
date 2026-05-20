package repository

import (
	"context"
	"sort"
	"sync"
	"time"

	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/domain"
)

type InMemoryLinkRepository struct {
	mu    sync.RWMutex
	links map[int64][]domain.Link
}

func NewInMemoryLinkRepository() *InMemoryLinkRepository {
	return &InMemoryLinkRepository{
		links: make(map[int64][]domain.Link),
	}
}

func (r *InMemoryLinkRepository) AddLink(ctx context.Context, chatID int64, link domain.Link) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.links[chatID] = append(r.links[chatID], link)
	return nil
}

func (r *InMemoryLinkRepository) RemoveLink(ctx context.Context, chatID int64, url string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	links := r.links[chatID]
	for i, link := range links {
		if link.URL == url {
			r.links[chatID] = append(links[:i], links[i+1:]...)
			return nil
		}
	}
	return nil
}

func (r *InMemoryLinkRepository) GetLinks(ctx context.Context, chatID int64) ([]domain.Link, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	links, exists := r.links[chatID]
	if !exists {
		return nil, nil
	}

	result := make([]domain.Link, len(links))
	copy(result, links)
	return result, nil
}

func (r *InMemoryLinkRepository) HasLink(ctx context.Context, chatID int64, url string) (bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, link := range r.links[chatID] {
		if link.URL == url {
			return true, nil
		}
	}
	return false, nil
}

func (r *InMemoryLinkRepository) GetLinksToCheck(ctx context.Context, limit int, before time.Time) ([]domain.Link, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	
	uniqueLinks := make(map[string]domain.Link)
	for _, chatLinks := range r.links {
		for _, link := range chatLinks {
			if link.LastCheckTime.Before(before) {
				uniqueLinks[link.URL] = link
			}
		}
	}

	var result []domain.Link
	for _, link := range uniqueLinks {
		result = append(result, link)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].LastCheckTime.Before(result[j].LastCheckTime)
	})

	if limit > 0 && len(result) > limit {
		result = result[:limit]
	}

	return result, nil
}

func (r *InMemoryLinkRepository) UpdateLink(ctx context.Context, updatedLink domain.Link) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	for chatID, chatLinks := range r.links {
		for i, link := range chatLinks {
			if link.URL == updatedLink.URL {
				r.links[chatID][i].LastCheckTime = updatedLink.LastCheckTime
				r.links[chatID][i].UpdatedAt = updatedLink.UpdatedAt
			}
		}
	}
	return nil
}

func (r *InMemoryLinkRepository) GetChatIDsForLink(ctx context.Context, linkID int64) ([]int64, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var chatIDs []int64
	for chatID, chatLinks := range r.links {
		for _, link := range chatLinks {
			if link.ID == linkID {
				chatIDs = append(chatIDs, chatID)
				break
			}
		}
	}
	return chatIDs, nil
}
