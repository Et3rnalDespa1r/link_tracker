package http

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"

	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/application"
)

type BotServer struct {
	srv    *http.Server
	logger *slog.Logger
	app    *application.BotService
}

func NewBotServer(address string, app *application.BotService, logger *slog.Logger) *BotServer {
	mux := http.NewServeMux()

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	return &BotServer{
		srv: &http.Server{
			Addr:    address,
			Handler: mux,
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
