package postgres

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/domain"
)

type SqlLinkRepository struct {
	pool *pgxpool.Pool
}

func NewSqlLinkRepository(pool *pgxpool.Pool) *SqlLinkRepository {
	return &SqlLinkRepository{
		pool: pool,
	}
}

func (r *SqlLinkRepository) AddLink(ctx context.Context, chatID int64, link domain.Link) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, "INSERT INTO chats (id) VALUES ($1) ON CONFLICT (id) DO NOTHING", chatID)
	if err != nil {
		return err
	}

	var linkID int64
	err = tx.QueryRow(ctx, `
		WITH e AS (
			INSERT INTO links (url) VALUES ($1)
			ON CONFLICT (url) DO NOTHING
			RETURNING id
		)
		SELECT id FROM e
		UNION ALL
		SELECT id FROM links WHERE url = $1
		LIMIT 1
	`, link.URL).Scan(&linkID)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, "INSERT INTO subscriptions (chat_id, link_id) VALUES ($1, $2) ON CONFLICT (chat_id, link_id) DO NOTHING", chatID, linkID)
	if err != nil {
		return err
	}

	for _, tag := range link.Tags {
		_, err = tx.Exec(ctx, "INSERT INTO tags (chat_id, link_id, name) VALUES ($1, $2, $3) ON CONFLICT (chat_id, link_id, name) DO NOTHING", chatID, linkID, tag)
		if err != nil {
			return err
		}
	}

	return tx.Commit(ctx)
}

func (r *SqlLinkRepository) RemoveLink(ctx context.Context, chatID int64, url string) error {
	query := `
		DELETE FROM subscriptions
		WHERE chat_id = $1 AND link_id = (SELECT id FROM links WHERE url = $2)
	`
	_, err := r.pool.Exec(ctx, query, chatID, url)
	return err
}

func (r *SqlLinkRepository) GetLinks(ctx context.Context, chatID int64) ([]domain.Link, error) {
	query := `
		SELECT l.url, COALESCE(t.name, '')
		FROM links l
		JOIN subscriptions s ON l.id = s.link_id
		LEFT JOIN tags t ON s.chat_id = t.chat_id AND s.link_id = t.link_id
		WHERE s.chat_id = $1
	`
	rows, err := r.pool.Query(ctx, query, chatID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	linkMap := make(map[string][]string)
	for rows.Next() {
		var url, tag string
		if err := rows.Scan(&url, &tag); err != nil {
			return nil, err
		}
		if tag != "" {
			linkMap[url] = append(linkMap[url], tag)
		} else if _, ok := linkMap[url]; !ok {
			linkMap[url] = []string{}
		}
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	var result []domain.Link
	for url, tags := range linkMap {
		result = append(result, domain.Link{URL: url, Tags: tags})
	}
	return result, nil
}

func (r *SqlLinkRepository) HasLink(ctx context.Context, chatID int64, url string) (bool, error) {
	var exists bool
	query := `
		SELECT EXISTS(
			SELECT 1 FROM subscriptions s
			JOIN links l ON s.link_id = l.id
			WHERE s.chat_id = $1 AND l.url = $2
		)
	`
	err := r.pool.QueryRow(ctx, query, chatID, url).Scan(&exists)
	return exists, err
}
