package http

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"

	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/infrastructure/cache"

	coreDomain "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/domain"
	scrapDomain "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/domain"
)

type LinkRepository interface {
	AddLink(ctx context.Context, chatID int64, link coreDomain.Link) error
	RemoveLink(ctx context.Context, chatID int64, url string) error
	GetLinks(ctx context.Context, chatID int64) ([]coreDomain.Link, error)
}

type ScrapperServer struct {
	srv    *http.Server
	logger *slog.Logger
	cache  *cache.ResponseCache
	repo   LinkRepository
}

func NewScrapperServer(address string, logger *slog.Logger, respCache *cache.ResponseCache, repo LinkRepository) *ScrapperServer {
	mux := http.NewServeMux()
	s := &ScrapperServer{
		srv: &http.Server{
			Addr:    address,
			Handler: mux,
		},
		logger: logger,
		cache:  respCache,
		repo:   repo,
	}

	mux.HandleFunc("POST /tg-chat/{id}", s.handleRegisterChat)
	mux.HandleFunc("DELETE /tg-chat/{id}", s.handleDeleteChat)
	mux.HandleFunc("POST /links", s.handleAddLink)
	mux.HandleFunc("DELETE /links", s.handleRemoveLink)
	mux.HandleFunc("GET /links", s.handleGetLinks)

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
	chatID, _ := strconv.ParseInt(r.Header.Get("Tg-Chat-Id"), 10, 64)

	cachedBytes, err := s.cache.GetList(r.Context(), chatID)
	if err == nil && cachedBytes != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(cachedBytes)
		return
	}

	links, err := s.repo.GetLinks(r.Context(), chatID)
	if err != nil {
		s.logger.Error("Ошибка получения ссылок из БД", "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	var responseLinks []scrapDomain.LinkResponse
	for _, link := range links {
		responseLinks = append(responseLinks, scrapDomain.LinkResponse{
			ID:  0,
			URL: link.URL,
		})
	}

	resp := scrapDomain.ListLinksResponse{Links: responseLinks, Size: len(responseLinks)}
	respBytes, _ := json.Marshal(resp)

	_ = s.cache.SetList(r.Context(), chatID, respBytes)

	w.Header().Set("Content-Type", "application/json")
	w.Write(respBytes)
}

type linkRequest struct {
	Link string `json:"link"`
}

func (s *ScrapperServer) handleAddLink(w http.ResponseWriter, r *http.Request) {
	chatID, _ := strconv.ParseInt(r.Header.Get("Tg-Chat-Id"), 10, 64)

	var req linkRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	linkToSave := coreDomain.Link{URL: req.Link}
	if err := s.repo.AddLink(r.Context(), chatID, linkToSave); err != nil {
		s.logger.Error("Ошибка добавления в БД", "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	_ = s.cache.Invalidate(r.Context(), chatID)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(scrapDomain.LinkResponse{ID: 0, URL: req.Link})
}

func (s *ScrapperServer) handleRemoveLink(w http.ResponseWriter, r *http.Request) {
	chatID, _ := strconv.ParseInt(r.Header.Get("Tg-Chat-Id"), 10, 64)

	var req linkRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	if err := s.repo.RemoveLink(r.Context(), chatID, req.Link); err != nil {
		s.logger.Error("Ошибка удаления из БД", "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	_ = s.cache.Invalidate(r.Context(), chatID)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(scrapDomain.LinkResponse{ID: 0, URL: req.Link})
}
