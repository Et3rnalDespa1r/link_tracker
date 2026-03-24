package postgres

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/application"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/domain"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/infrastructure/db"
)

func setupTestDB(t *testing.T) (*pgxpool.Pool, func()) {
	ctx := context.Background()

	pgContainer, err := postgres.RunContainer(ctx,
		testcontainers.WithImage("postgres:15-alpine"),
		postgres.WithDatabase("linktracker_test"),
		postgres.WithUsername("testuser"),
		postgres.WithPassword("testpass"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(10*time.Second),
		),
	)
	require.NoError(t, err)

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	err = db.RunMigrations(connStr, logger)
	require.NoError(t, err, "Миграции должны примениться успешно")

	pool, err := pgxpool.New(ctx, connStr)
	require.NoError(t, err)

	cleanup := func() {
		pool.Close()
		if err := pgContainer.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate pgContainer: %s", err)
		}
	}

	return pool, cleanup
}

func TestRepositories(t *testing.T) {
	originalWD, _ := os.Getwd()
	os.Chdir("../../../../")
	defer os.Chdir(originalWD)

	pool, cleanup := setupTestDB(t)
	defer cleanup()

	repos := map[string]application.LinkRepository{
		"Raw SQL":       NewSqlLinkRepository(pool),
		"Query Builder": NewBuilderLinkRepository(pool),
	}

	for name, repo := range repos {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			chatID := int64(999999)

			link := domain.Link{
				URL:  "https://github.com/testcontainers",
				Tags: []string{"testing", "docker"},
			}

			// 1. Добавление ссылки
			err := repo.AddLink(ctx, chatID, link)
			assert.NoError(t, err)

			// 2. Проверка, что дублирующая ссылка не вызывает ошибку (DO NOTHING в SQL)
			err = repo.AddLink(ctx, chatID, link)
			assert.NoError(t, err)

			// 3. Проверка существования (HasLink)
			exists, err := repo.HasLink(ctx, chatID, link.URL)
			assert.NoError(t, err)
			assert.True(t, exists)

			// 4. Получение списка (GetLinks)
			links, err := repo.GetLinks(ctx, chatID)
			assert.NoError(t, err)
			assert.Len(t, links, 1)
			assert.Equal(t, link.URL, links[0].URL)
			assert.ElementsMatch(t, link.Tags, links[0].Tags) // Проверка тегов

			// 5. Удаление ссылки
			err = repo.RemoveLink(ctx, chatID, link.URL)
			assert.NoError(t, err)

			// 6. Проверка, что ссылка удалилась
			exists, err = repo.HasLink(ctx, chatID, link.URL)
			assert.NoError(t, err)
			assert.False(t, exists)

			// 7. Проверка, что список пуст
			links, err = repo.GetLinks(ctx, chatID)
			assert.NoError(t, err)
			assert.Len(t, links, 0)
		})
	}
}
