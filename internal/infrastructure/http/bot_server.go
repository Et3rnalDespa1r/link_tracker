package http

import (
	"context"
	"encoding/json"
	"fmt"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/metrics"
	"io"
	"log/slog"
	"net/http"
	"time"

	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/domain"
)

type Notifier interface {
	Notify(context.Context, domain.LinkUpdate) error
}

type BotServer struct {
	srv    *http.Server
	logger *slog.Logger
	app    Notifier
}

func NewBotServer(address string, app Notifier, logger *slog.Logger) *BotServer {
	mux := http.NewServeMux()
	s := &BotServer{logger: logger, app: app}
	mux.HandleFunc("POST /updates", s.handleUpdates)

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	return &BotServer{
		srv: &http.Server{ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second,
			Addr:    address,
			Handler: metrics.Instrument(mux),
		},
		logger: logger,
		app:    app,
	}
}

func (s *BotServer) Start() error {
	s.logger.Info("Запуск HTTP сервера Bot", slog.String("address", s.srv.Addr))
	if err := s.srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("bot http server error: %w", err)
	}
	return nil
}

func (s *BotServer) Stop(ctx context.Context) error {
	s.logger.Info("Остановка HTTP сервера Bot...")
	return s.srv.Shutdown(ctx)
}

func (s *BotServer) handleUpdates(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	decoder := json.NewDecoder(r.Body)
	var update domain.LinkUpdate
	if err := decoder.Decode(&update); err != nil {
		http.Error(w, "invalid update", http.StatusBadRequest)
		return
	}
	if err := decoder.Decode(new(any)); err != io.EOF || update.ID <= 0 || update.URL == "" || update.Description == "" || len(update.TgChatIDs) == 0 {
		http.Error(w, "invalid update", http.StatusBadRequest)
		return
	}
	for _, id := range update.TgChatIDs {
		if id == 0 {
			http.Error(w, "invalid chat id", http.StatusBadRequest)
			return
		}
	}
	if err := s.app.Notify(r.Context(), update); err != nil {
		s.logger.Error("Ошибка отправки обновления", "error", err)
		http.Error(w, "notification failed", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}
