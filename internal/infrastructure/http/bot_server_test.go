package http

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/domain"
)

type mockNotifier struct{}

func (m *mockNotifier) Notify(ctx context.Context, update domain.LinkUpdate) error {
	return nil
}

func TestBotServer_HandleUpdates_Valid(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	server := NewBotServer(":0", &mockNotifier{}, logger)

	update := domain.LinkUpdate{
		ID:          1,
		URL:         "https://github.com/test",
		Description: "test update",
		TgChatIDs:   []int64{123},
	}
	body, _ := json.Marshal(update)
	req := httptest.NewRequest(http.MethodPost, "/updates", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	server.srv.Handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}

func TestBotServer_HandleUpdates_InvalidData(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	server := NewBotServer(":0", &mockNotifier{}, logger)

	update := domain.LinkUpdate{
		ID: 1,
	}
	body, _ := json.Marshal(update)
	req := httptest.NewRequest(http.MethodPost, "/updates", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	server.srv.Handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}
