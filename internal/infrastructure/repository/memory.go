package repository

import (
	"context"
	"sync"

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
