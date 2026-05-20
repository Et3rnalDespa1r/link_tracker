package http

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"sync"

	"golang.org/x/time/rate"

	coreDomain "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/domain"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/infrastructure/cache"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/infrastructure/repository/postgres"
	scrapDomain "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/domain"
)

type IPRateLimiter struct {
	ips map[string]*rate.Limiter
	mu  sync.RWMutex
	r   rate.Limit
	b   int
}

func NewIPRateLimiter(r rate.Limit, b int) *IPRateLimiter {
	return &IPRateLimiter{
		ips: make(map[string]*rate.Limiter),
		r:   r,
		b:   b,
	}
}

func (i *IPRateLimiter) GetLimiter(ip string) *rate.Limiter {
	i.mu.RLock()
	limiter, exists := i.ips[ip]
	i.mu.RUnlock()

	if !exists {
		i.mu.Lock()
		defer i.mu.Unlock()
		limiter, exists = i.ips[ip]
		if !exists {
			limiter = rate.NewLimiter(i.r, i.b)
			i.ips[ip] = limiter
		}
		return limiter
	}
	return limiter
}

func rateLimitMiddleware(limiter *IPRateLimiter, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ip, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			ip = r.RemoteAddr
		}

		if !limiter.GetLimiter(ip).Allow() {
			http.Error(w, "Too Many Requests", http.StatusTooManyRequests)
			return
		}

		next.ServeHTTP(w, r)
	})
}

type ScrapperServer struct {
	srv    *http.Server
	logger *slog.Logger
	cache  *cache.ResponseCache
	repo   *postgres.SqlLinkRepository
}

func NewScrapperServer(address string, rps float64, burst int, logger *slog.Logger, respCache *cache.ResponseCache, repo *postgres.SqlLinkRepository) *ScrapperServer {
	mux := http.NewServeMux()
	s := &ScrapperServer{
		logger: logger,
		cache:  respCache,
		repo:   repo,
	}

	mux.HandleFunc("POST /tg-chat/{id}", s.handleRegisterChat)
	mux.HandleFunc("DELETE /tg-chat/{id}", s.handleDeleteChat)
	mux.HandleFunc("GET /links", s.handleGetLinks)
	mux.HandleFunc("POST /links", s.handleAddLink)
	mux.HandleFunc("DELETE /links", s.handleRemoveLink)

	limiter := NewIPRateLimiter(rate.Limit(rps), burst)
	handlerWithRateLimit := rateLimitMiddleware(limiter, mux)

	s.srv = &http.Server{
		Addr:    address,
		Handler: handlerWithRateLimit,
	}

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
	chatID, err := strconv.ParseInt(r.Header.Get("Tg-Chat-Id"), 10, 64)
	if err != nil || chatID <= 0 {
		http.Error(w, "invalid Tg-Chat-Id header", http.StatusBadRequest)
		return
	}

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

	responseLinks := make([]scrapDomain.LinkResponse, 0, len(links))
	for _, link := range links {
		responseLinks = append(responseLinks, scrapDomain.LinkResponse{ID: link.ID, URL: link.URL})
	}

	resp := scrapDomain.ListLinksResponse{Links: responseLinks, Size: len(responseLinks)}
	respBytes, err := json.Marshal(resp)
	if err != nil {
		s.logger.Error("Ошибка сериализации ответа", "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	_ = s.cache.SetList(r.Context(), chatID, respBytes)

	w.Header().Set("Content-Type", "application/json")
	w.Write(respBytes)
}

func (s *ScrapperServer) handleAddLink(w http.ResponseWriter, r *http.Request) {
	chatID, err := strconv.ParseInt(r.Header.Get("Tg-Chat-Id"), 10, 64)
	if err != nil || chatID <= 0 {
		http.Error(w, "invalid Tg-Chat-Id header", http.StatusBadRequest)
		return
	}

	var req scrapDomain.AddLinkRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Link == "" {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	linkToSave := coreDomain.Link{URL: req.Link}
	if err := s.repo.AddLink(r.Context(), chatID, linkToSave); err != nil {
		s.logger.Error("Ошибка добавления ссылки", "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	_ = s.cache.Invalidate(r.Context(), chatID)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(scrapDomain.LinkResponse{ID: 0, URL: req.Link})
}

func (s *ScrapperServer) handleRemoveLink(w http.ResponseWriter, r *http.Request) {
	chatID, err := strconv.ParseInt(r.Header.Get("Tg-Chat-Id"), 10, 64)
	if err != nil || chatID <= 0 {
		http.Error(w, "invalid Tg-Chat-Id header", http.StatusBadRequest)
		return
	}

	var req scrapDomain.RemoveLinkRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Link == "" {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if err := s.repo.RemoveLink(r.Context(), chatID, req.Link); err != nil {
		s.logger.Error("Ошибка удаления ссылки", "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	_ = s.cache.Invalidate(r.Context(), chatID)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(scrapDomain.LinkResponse{ID: 0, URL: req.Link})
}
