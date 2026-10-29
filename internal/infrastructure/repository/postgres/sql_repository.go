package postgres

import (
	"context"
	"encoding/json"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/domain"
	"net/url"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	scrapDomain "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/domain"
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
       INSERT INTO links (url, last_check_time) 
       VALUES ($1, '1970-01-01 00:00:00+00')
       ON CONFLICT (url) DO UPDATE SET url = EXCLUDED.url
       RETURNING id
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
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, `
        DELETE FROM subscriptions 
        WHERE chat_id = $1 AND link_id = (SELECT id FROM links WHERE url = $2)`,
		chatID, url)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `
        DELETE FROM links 
        WHERE url = $1 AND id NOT IN (SELECT link_id FROM subscriptions)`,
		url)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (r *SqlLinkRepository) GetLinks(ctx context.Context, chatID int64) ([]domain.Link, error) {
	query := `
		SELECT l.id, l.url, COALESCE(t.name, '')
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
	ids := make(map[string]int64)
	for rows.Next() {
		var id int64
		var url, tag string
		if err := rows.Scan(&id, &url, &tag); err != nil {
			return nil, err
		}
		ids[url] = id
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
		result = append(result, domain.Link{ID: ids[url], URL: url, Tags: tags})
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

func (r *SqlLinkRepository) GetLinksToCheck(ctx context.Context, limit int, before time.Time) ([]domain.Link, error) {
	query := `
		SELECT id, url, last_check_time, updated_at 
		FROM links 
		WHERE last_check_time < $1 
		ORDER BY last_check_time ASC 
		LIMIT $2
	`
	rows, err := r.pool.Query(ctx, query, before, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var links []domain.Link
	for rows.Next() {
		var l domain.Link
		if err := rows.Scan(&l.ID, &l.URL, &l.LastCheckTime, &l.UpdatedAt); err != nil {
			return nil, err
		}
		links = append(links, l)
	}
	return links, rows.Err()
}

func (r *SqlLinkRepository) UpdateLink(ctx context.Context, link domain.Link) error {
	query := `
		UPDATE links 
		SET last_check_time = $1, updated_at = $2 
		WHERE id = $3
	`
	_, err := r.pool.Exec(ctx, query, link.LastCheckTime, link.UpdatedAt, link.ID)
	return err
}

func (r *SqlLinkRepository) GetChatIDsForLink(ctx context.Context, linkID int64) ([]int64, error) {
	query := `SELECT chat_id FROM subscriptions WHERE link_id = $1`
	rows, err := r.pool.Query(ctx, query, linkID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var chatIDs []int64
	for rows.Next() {
		var chatID int64
		if err := rows.Scan(&chatID); err != nil {
			return nil, err
		}
		chatIDs = append(chatIDs, chatID)
	}
	return chatIDs, rows.Err()
}

func (r *SqlLinkRepository) UpdateLinkAndSaveOutbox(ctx context.Context, link domain.Link, update scrapDomain.LinkUpdate) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, `UPDATE links SET last_check_time = $1, updated_at = $2 WHERE id = $3`, link.LastCheckTime, link.UpdatedAt, link.ID)
	if err != nil {
		return err
	}

	payload, _ := json.Marshal(update)
	_, err = tx.Exec(ctx, `INSERT INTO link_updates_outbox (payload, status) VALUES ($1, 'PENDING')`, payload)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (r *SqlLinkRepository) RegisterChat(ctx context.Context, id int64) error {
	_, err := r.pool.Exec(ctx, "INSERT INTO chats(id) VALUES($1) ON CONFLICT DO NOTHING", id)
	return err
}
func (r *SqlLinkRepository) ChatExists(ctx context.Context, id int64) (bool, error) {
	var exists bool
	err := r.pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM chats WHERE id=$1)", id).Scan(&exists)
	return exists, err
}
func (r *SqlLinkRepository) DeleteChat(ctx context.Context, id int64) (bool, error) {
	result, err := r.pool.Exec(ctx, "DELETE FROM chats WHERE id=$1", id)
	return result.RowsAffected() > 0, err
}

func (r *SqlLinkRepository) TrackedSources(ctx context.Context) (map[string]int64, error) {
	rows, err := r.pool.Query(ctx, "SELECT url FROM links WHERE EXISTS (SELECT 1 FROM subscriptions WHERE link_id=links.id)")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := map[string]int64{"github.com": 0, "stackoverflow.com": 0}
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		u, err := url.Parse(raw)
		if err == nil {
			result[u.Hostname()]++
		}
	}
	return result, rows.Err()
}
