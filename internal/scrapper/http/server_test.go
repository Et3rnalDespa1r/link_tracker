package http

import (
	"context"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/infrastructure/cache"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/infrastructure/repository/postgres"
	"log/slog"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

func TestChatAndLinkContract(t *testing.T) {
	ctx := context.Background()
	container, err := tcpostgres.RunContainer(ctx, testcontainers.WithImage("postgres:15-alpine"), tcpostgres.WithDatabase("test"), tcpostgres.WithUsername("test"), tcpostgres.WithPassword("test"), testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(30*time.Second)))
	require.NoError(t, err)
	defer container.Terminate(ctx)
	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)
	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	defer pool.Close()
	schema, err := os.ReadFile("../../../migrations/000001_init.up.sql")
	require.NoError(t, err)
	_, err = pool.Exec(ctx, string(schema))
	require.NoError(t, err)
	server := NewScrapperServer(":0", 1000, 1000, slog.Default(), cache.NewResponseCache(nil, time.Minute), postgres.NewSqlLinkRepository(pool))
	request := func(method, path, body string, status int) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		req.Header.Set("Tg-Chat-Id", "-123")
		response := httptest.NewRecorder()
		server.srv.Handler.ServeHTTP(response, req)
		require.Equal(t, status, response.Code, response.Body.String())
		return response
	}
	request("POST", "/links", `{"link":"https://github.com/a/b"}`, 404)
	request("POST", "/tg-chat/-123", "", 200)
	request("POST", "/links", `{"link":"https://github.com/a/b","tags":["work"]}`, 200)
	response := request("GET", "/links", "", 200)
	require.Contains(t, response.Body.String(), `"tags":["work"]`)
	require.NotContains(t, response.Body.String(), `"id":0`)
	request("DELETE", "/tg-chat/-123", "", 200)
	request("GET", "/links", "", 404)
	request("DELETE", "/tg-chat/-123", "", 404)
}
