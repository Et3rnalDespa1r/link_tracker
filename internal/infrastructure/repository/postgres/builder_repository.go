package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5/pgxpool"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/domain"
	scrapDomain "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/domain"
)

type BuilderLinkRepository struct {
	pool *pgxpool.Pool
	psql squirrel.StatementBuilderType
}

func NewBuilderLinkRepository(pool *pgxpool.Pool) *BuilderLinkRepository {
	return &BuilderLinkRepository{
		pool: pool,
		psql: squirrel.StatementBuilder.PlaceholderFormat(squirrel.Dollar),
	}
}

func (r *BuilderLinkRepository) AddLink(ctx context.Context, chatID int64, link domain.Link) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	chatQuery, chatArgs, _ := r.psql.Insert("chats").
		Columns("id").
		Values(chatID).
		Suffix("ON CONFLICT (id) DO NOTHING").
		ToSql()
	_, err = tx.Exec(ctx, chatQuery, chatArgs...)
	if err != nil {
		return err
	}

	var linkID int64
	linkQuery, linkArgs, _ := r.psql.Insert("links").
		Columns("url", "last_check_time").
		Values(link.URL, time.Unix(0, 0)).
		Suffix("ON CONFLICT (url) DO UPDATE SET url = EXCLUDED.url RETURNING id").
		ToSql()
	err = tx.QueryRow(ctx, linkQuery, linkArgs...).Scan(&linkID)
	if err != nil {
		return err
	}

	subQuery, subArgs, _ := r.psql.Insert("subscriptions").
		Columns("chat_id", "link_id").
		Values(chatID, linkID).
		Suffix("ON CONFLICT (chat_id, link_id) DO NOTHING").
		ToSql()
	_, err = tx.Exec(ctx, subQuery, subArgs...)
	if err != nil {
		return err
	}

	if len(link.Tags) > 0 {
		tagBuilder := r.psql.Insert("tags").Columns("chat_id", "link_id", "name")
		for _, tag := range link.Tags {
			tagBuilder = tagBuilder.Values(chatID, linkID, tag)
		}
		tagQuery, tagArgs, _ := tagBuilder.Suffix("ON CONFLICT (chat_id, link_id, name) DO NOTHING").ToSql()
		_, err = tx.Exec(ctx, tagQuery, tagArgs...)
		if err != nil {
			return err
		}
	}

	return tx.Commit(ctx)
}

func (r *BuilderLinkRepository) RemoveLink(ctx context.Context, chatID int64, url string) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	query, args, _ := r.psql.Delete("subscriptions").
		Where(squirrel.Eq{"chat_id": chatID}).
		Where(squirrel.Expr("link_id = (SELECT id FROM links WHERE url = ?)", url)).
		ToSql()

	if _, err := tx.Exec(ctx, query, args...); err != nil {
		return fmt.Errorf("delete subscription: %w", err)
	}

	cleanupQuery, args, _ := r.psql.Delete("links").
		Where(squirrel.Eq{"url": url}).
		Where(squirrel.Expr("id NOT IN (SELECT link_id FROM subscriptions)")).
		ToSql()

	if _, err := tx.Exec(ctx, cleanupQuery, args...); err != nil {
		return fmt.Errorf("cleanup link: %w", err)
	}

	return tx.Commit(ctx)
}

func (r *BuilderLinkRepository) GetLinks(ctx context.Context, chatID int64) ([]domain.Link, error) {
	query, args, _ := r.psql.Select("l.url", "COALESCE(t.name, '')").
		From("links l").
		Join("subscriptions s ON l.id = s.link_id").
		LeftJoin("tags t ON s.chat_id = t.chat_id AND s.link_id = t.link_id").
		Where(squirrel.Eq{"s.chat_id": chatID}).
		ToSql()

	rows, err := r.pool.Query(ctx, query, args...)
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

	var result []domain.Link
	for url, tags := range linkMap {
		result = append(result, domain.Link{URL: url, Tags: tags})
	}
	return result, nil
}

func (r *BuilderLinkRepository) HasLink(ctx context.Context, chatID int64, url string) (bool, error) {
	query, args, _ := r.psql.Select("1").
		From("subscriptions s").
		Join("links l ON s.link_id = l.id").
		Where(squirrel.Eq{"s.chat_id": chatID, "l.url": url}).
		Limit(1).
		ToSql()

	var exists int
	err := r.pool.QueryRow(ctx, query, args...).Scan(&exists)
	if err != nil {
		if err.Error() == "no rows in result set" {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (r *BuilderLinkRepository) GetLinksToCheck(ctx context.Context, limit int, before time.Time) ([]domain.Link, error) {
	query, args, _ := r.psql.Select("id", "url", "last_check_time", "updated_at").
		From("links").
		Where(squirrel.Lt{"last_check_time": before}).
		OrderBy("last_check_time ASC").
		Limit(uint64(limit)).
		ToSql()

	rows, err := r.pool.Query(ctx, query, args...)
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

func (r *BuilderLinkRepository) UpdateLink(ctx context.Context, link domain.Link) error {
	query, args, _ := r.psql.Update("links").
		Set("last_check_time", link.LastCheckTime).
		Set("updated_at", link.UpdatedAt).
		Where(squirrel.Eq{"id": link.ID}).
		ToSql()

	_, err := r.pool.Exec(ctx, query, args...)
	return err
}

func (r *BuilderLinkRepository) GetChatIDsForLink(ctx context.Context, linkID int64) ([]int64, error) {
	query, args, _ := r.psql.Select("chat_id").
		From("subscriptions").
		Where(squirrel.Eq{"link_id": linkID}).
		ToSql()

	rows, err := r.pool.Query(ctx, query, args...)
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

func (r *BuilderLinkRepository) UpdateLinkAndSaveOutbox(ctx context.Context, link domain.Link, update scrapDomain.LinkUpdate) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	updateQuery, updateArgs, _ := r.psql.Update("links").
		Set("last_check_time", link.LastCheckTime).
		Set("updated_at", link.UpdatedAt).
		Where(squirrel.Eq{"id": link.ID}).
		ToSql()

	if _, err := tx.Exec(ctx, updateQuery, updateArgs...); err != nil {
		return err
	}

	payload, _ := json.Marshal(update)
	insertQuery, insertArgs, _ := r.psql.Insert("link_updates_outbox").
		Columns("payload", "status").
		Values(payload, "PENDING").
		ToSql()

	if _, err := tx.Exec(ctx, insertQuery, insertArgs...); err != nil {
		return err
	}

	return tx.Commit(ctx)
}
