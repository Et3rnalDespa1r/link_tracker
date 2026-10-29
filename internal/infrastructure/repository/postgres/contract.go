package postgres

import (
	"context"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/application"
	coreDomain "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/domain"
	scrapDomain "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/domain"
)

type Repository interface {
	application.LinkRepository
	UpdateLinkAndSaveOutbox(context.Context, coreDomain.Link, scrapDomain.LinkUpdate) error
	RegisterChat(context.Context, int64) error
	ChatExists(context.Context, int64) (bool, error)
	DeleteChat(context.Context, int64) (bool, error)
	TrackedSources(context.Context) (map[string]int64, error)
}

func (r *BuilderLinkRepository) RegisterChat(ctx context.Context, id int64) error {
	return NewSqlLinkRepository(r.pool).RegisterChat(ctx, id)
}
func (r *BuilderLinkRepository) ChatExists(ctx context.Context, id int64) (bool, error) {
	return NewSqlLinkRepository(r.pool).ChatExists(ctx, id)
}
func (r *BuilderLinkRepository) DeleteChat(ctx context.Context, id int64) (bool, error) {
	return NewSqlLinkRepository(r.pool).DeleteChat(ctx, id)
}
func (r *BuilderLinkRepository) TrackedSources(ctx context.Context) (map[string]int64, error) {
	return NewSqlLinkRepository(r.pool).TrackedSources(ctx)
}
