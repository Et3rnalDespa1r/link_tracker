package kafka_test

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcPostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/domain"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/infrastructure/db"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/infrastructure/repository/postgres"
	myKafka "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/client/kafka"
	scrapDomain "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/domain"
)

type MockMessageSender struct {
	shouldError bool
	sendCount   int
}

func (m *MockMessageSender) SendUpdate(ctx context.Context, update scrapDomain.LinkUpdate) error {
	m.sendCount++
	if m.shouldError {
		return errors.New("kafka is down")
	}
	return nil
}

func setupTestDB(t *testing.T) (*pgxpool.Pool, func()) {
	ctx := context.Background()

	pgContainer, err := tcPostgres.RunContainer(ctx,
		testcontainers.WithImage("postgres:15-alpine"),
		tcPostgres.WithDatabase("linktracker_test"),
		tcPostgres.WithUsername("testuser"),
		tcPostgres.WithPassword("testpass"),
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
	originalWD, _ := os.Getwd()
	os.Chdir("../../../../")
	err = db.RunMigrations(connStr, logger)
	os.Chdir(originalWD)
	require.NoError(t, err)

	pool, err := pgxpool.New(ctx, connStr)
	require.NoError(t, err)

	cleanup := func() {
		pool.Close()
		pgContainer.Terminate(ctx)
	}

	return pool, cleanup
}

func TestOutboxProcessor_Integration(t *testing.T) {
	pool, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	outboxRepo := postgres.NewOutboxRepository(pool)
	mockSender := &MockMessageSender{shouldError: true}

	processor := myKafka.NewOutboxProcessor(outboxRepo, mockSender, logger)
	processorCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go processor.Run(processorCtx)

	repo := postgres.NewSqlLinkRepository(pool)
	err := repo.AddLink(ctx, 1, domain.Link{URL: "https://test.com"})
	require.NoError(t, err)

	update := scrapDomain.LinkUpdate{
		ID:          1,
		URL:         "https://test.com",
		Description: "Test update",
		TgChatIDs:   []int64{1},
	}
	link := domain.Link{ID: 1, LastCheckTime: time.Now(), UpdatedAt: time.Now()}

	err = repo.UpdateLinkAndSaveOutbox(ctx, link, update)
	require.NoError(t, err)

	time.Sleep(3 * time.Second)

	var status string
	err = pool.QueryRow(ctx, "SELECT status FROM link_updates_outbox LIMIT 1").Scan(&status)
	require.NoError(t, err)
	assert.Equal(t, "PENDING", status)

	mockSender.shouldError = false

	assert.Eventually(t, func() bool {
		var s string
		err := pool.QueryRow(ctx, "SELECT status FROM link_updates_outbox LIMIT 1").Scan(&s)
		return err == nil && s == "PROCESSED"
	}, 10*time.Second, 1*time.Second)

	assert.Greater(t, mockSender.sendCount, 0)
}
