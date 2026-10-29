package postgres

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

type OutboxRepository struct {
	pool *pgxpool.Pool
}

func NewOutboxRepository(pool *pgxpool.Pool) *OutboxRepository {
	return &OutboxRepository{pool: pool}
}

func (r *OutboxRepository) FetchPending(ctx context.Context) (map[int64][]byte, error) {
	rows, err := r.pool.Query(ctx, "SELECT id, payload FROM link_updates_outbox WHERE status = 'PENDING' ORDER BY created_at ASC LIMIT 50")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	res := make(map[int64][]byte)
	for rows.Next() {
		var id int64
		var payload []byte
		if err := rows.Scan(&id, &payload); err != nil {
			return nil, err
		}
		res[id] = payload
	}
	return res, rows.Err()
}

func (r *OutboxRepository) MarkProcessed(ctx context.Context, id int64) error {
	_, err := r.pool.Exec(ctx, "UPDATE link_updates_outbox SET status = 'PROCESSED' WHERE id = $1", id)
	return err
}
