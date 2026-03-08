package http

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"

	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/domain"
)

type UpdateNotifier interface {
	Notify(ctx context.Context, update domain.LinkUpdate) error
}

type BotServer struct {
	srv      *http.Server
	notifier UpdateNotifier
	logger   *slog.Logger
}

func NewBotServer(address string, notifier UpdateNotifier, logger *slog.Logger) *BotServer {
	mux := http.NewServeMux()
	s := &BotServer{
		srv: &http.Server{
			Addr:    address,
			Handler: mux,
		},
		notifier: notifier,
		logger:   logger,
	}

	mux.HandleFunc("/updates", s.handleUpdates)
	return s
}

func (s *BotServer) Start() error {
	s.logger.Info("Запуск HTTP сервера бота", slog.String("address", s.srv.Addr))
	if err := s.srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("http server error: %w", err)
	}
	return nil
}

func (s *BotServer) Stop(ctx context.Context) error {
	s.logger.Info("Остановка HTTP сервера бота...")
	return s.srv.Shutdown(ctx)
}

func (s *BotServer) handleUpdates(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var update domain.LinkUpdate
	if err := json.NewDecoder(r.Body).Decode(&update); err != nil {
		s.logger.Error("Ошибка декодирования запроса", slog.Any("error", err))
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}

	if update.URL == "" || len(update.TgChatIDs) == 0 {
		http.Error(w, "Invalid update payload", http.StatusBadRequest)
		return
	}

	if err := s.notifier.Notify(r.Context(), update); err != nil {
		s.logger.Error("Ошибка обработки обновления", slog.Any("error", err))
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}
