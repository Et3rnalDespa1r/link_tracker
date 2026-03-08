package http

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"

	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/domain"
)

type ScrapperServer struct {
	srv    *http.Server
	logger *slog.Logger
}

func NewScrapperServer(address string, logger *slog.Logger) *ScrapperServer {
	mux := http.NewServeMux()
	s := &ScrapperServer{
		srv: &http.Server{
			Addr:    address,
			Handler: mux,
		},
		logger: logger,
	}

	mux.HandleFunc("POST /tg-chat/{id}", s.handleRegisterChat)
	mux.HandleFunc("DELETE /tg-chat/{id}", s.handleDeleteChat)
	mux.HandleFunc("GET /links", s.handleGetLinks)
	mux.HandleFunc("POST /links", s.handleAddLink)
	mux.HandleFunc("DELETE /links", s.handleRemoveLink)

	return s
}

func (s *ScrapperServer) Start() error {
	s.logger.Info("Запуск HTTP сервера Scrapper", slog.String("address", s.srv.Addr))
	if err := s.srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("http server error: %w", err)
	}
	return nil
}

func (s *ScrapperServer) Stop(ctx context.Context) error {
	s.logger.Info("Остановка HTTP сервера Scrapper...")
	return s.srv.Shutdown(ctx)
}

func (s *ScrapperServer) handleRegisterChat(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func (s *ScrapperServer) handleDeleteChat(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func (s *ScrapperServer) handleGetLinks(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(domain.ListLinksResponse{Links: []domain.LinkResponse{}, Size: 0})
}

func (s *ScrapperServer) handleAddLink(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(domain.LinkResponse{ID: 1, URL: "https://example.com/new-link"})
}

func (s *ScrapperServer) handleRemoveLink(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(domain.LinkResponse{ID: 1, URL: "https://example.com/removed-link"})
}
