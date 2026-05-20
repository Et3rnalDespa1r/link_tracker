package application

import (
	"context"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/domain"
	"time"
)

type LinkRepository interface {
	AddLink(ctx context.Context, chatID int64, link domain.Link) error
	RemoveLink(ctx context.Context, chatID int64, url string) error
	GetLinks(ctx context.Context, chatID int64) ([]domain.Link, error)
	HasLink(ctx context.Context, chatID int64, url string) (bool, error)
	GetLinksToCheck(ctx context.Context, limit int, before time.Time) ([]domain.Link, error)
	UpdateLink(ctx context.Context, link domain.Link) error
	GetChatIDsForLink(ctx context.Context, linkID int64) ([]int64, error)
}
