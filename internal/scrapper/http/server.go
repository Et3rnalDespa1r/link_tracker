package http

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/parser"
	"golang.org/x/time/rate"

	coreDomain "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/domain"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/infrastructure/cache"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/infrastructure/repository/postgres"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/metrics"
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
	repo   postgres.Repository
}

func NewScrapperServer(address string, rps float64, burst int, logger *slog.Logger, respCache *cache.ResponseCache, repo postgres.Repository) *ScrapperServer {
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

	mux.Handle("GET /metrics", promhttp.Handler())

	limiter := NewIPRateLimiter(rate.Limit(rps), burst)
	handlerWithRateLimit := rateLimitMiddleware(limiter, mux)

	s.srv = &http.Server{ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second,
		Addr:    address,
		Handler: metrics.Instrument(handlerWithRateLimit),
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
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id == 0 {
		http.Error(w, "invalid chat id", 400)
		return
	}
	if r.Method == http.MethodPost {
		if err := s.repo.RegisterChat(r.Context(), id); err != nil {
			http.Error(w, "database error", 500)
			return
		}
	} else {
		deleted, err := s.repo.DeleteChat(r.Context(), id)
		if err != nil {
			http.Error(w, "database error", 500)
			return
		}
		if !deleted {
			http.Error(w, "chat not found", 404)
			return
		}
		_ = s.cache.Invalidate(r.Context(), id)
	}
	w.WriteHeader(http.StatusOK)
}

func (s *ScrapperServer) handleDeleteChat(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id == 0 {
		http.Error(w, "invalid chat id", 400)
		return
	}
	if r.Method == http.MethodPost {
		if err := s.repo.RegisterChat(r.Context(), id); err != nil {
			http.Error(w, "database error", 500)
			return
		}
	} else {
		deleted, err := s.repo.DeleteChat(r.Context(), id)
		if err != nil {
			http.Error(w, "database error", 500)
			return
		}
		if !deleted {
			http.Error(w, "chat not found", 404)
			return
		}
		_ = s.cache.Invalidate(r.Context(), id)
	}
	w.WriteHeader(http.StatusOK)
}

func (s *ScrapperServer) handleGetLinks(w http.ResponseWriter, r *http.Request) {
	metrics.ApiRequests.WithLabelValues("tg_bot").Inc()

	chatID, err := strconv.ParseInt(r.Header.Get("Tg-Chat-Id"), 10, 64)
	if err != nil || chatID == 0 {
		http.Error(w, "invalid Tg-Chat-Id header", http.StatusBadRequest)
		return
	}

	if !s.requireChat(w, r, chatID) {
		return
	}

	cachedBytes, err := s.cache.GetList(r.Context(), chatID)
	if err == nil && cachedBytes != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(cachedBytes)
		return
	}

	start := time.Now()
	links, err := s.repo.GetLinks(r.Context(), chatID)
	duration := time.Since(start).Milliseconds()
	metrics.RequestDuration.WithLabelValues("database", "links_table").Observe(float64(duration))

	if err != nil {
		s.logger.Error("Ошибка получения ссылок из БД", "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	responseLinks := make([]scrapDomain.LinkResponse, 0, len(links))
	for _, link := range links {
		responseLinks = append(responseLinks, scrapDomain.LinkResponse{ID: link.ID, URL: link.URL, Tags: link.Tags})
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
	metrics.ApiRequests.WithLabelValues("tg_bot").Inc()

	chatID, err := strconv.ParseInt(r.Header.Get("Tg-Chat-Id"), 10, 64)
	if err != nil || chatID == 0 {
		http.Error(w, "invalid Tg-Chat-Id header", http.StatusBadRequest)
		return
	}

	if !s.requireChat(w, r, chatID) {
		return
	}

	var req scrapDomain.AddLinkRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Link == "" {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if _, err := parser.ParseURL(req.Link); err != nil {
		http.Error(w, "unsupported URL", 400)
		return
	}
	start := time.Now()
	linkToSave := coreDomain.Link{URL: req.Link, Tags: req.Tags}
	err = s.repo.AddLink(r.Context(), chatID, linkToSave)
	duration := time.Since(start).Milliseconds()
	metrics.RequestDuration.WithLabelValues("database", "links_table").Observe(float64(duration))

	if err != nil {
		s.logger.Error("Ошибка добавления ссылки", "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	_ = s.cache.Invalidate(r.Context(), chatID)

	w.Header().Set("Content-Type", "application/json")
	links, _ := s.repo.GetLinks(r.Context(), chatID)
	for _, link := range links {
		if link.URL == req.Link {
			json.NewEncoder(w).Encode(scrapDomain.LinkResponse{ID: link.ID, URL: link.URL, Tags: link.Tags})
			return
		}
	}
	json.NewEncoder(w).Encode(scrapDomain.LinkResponse{URL: req.Link})
}

func (s *ScrapperServer) handleRemoveLink(w http.ResponseWriter, r *http.Request) {
	metrics.ApiRequests.WithLabelValues("tg_bot").Inc()

	chatID, err := strconv.ParseInt(r.Header.Get("Tg-Chat-Id"), 10, 64)
	if err != nil || chatID == 0 {
		http.Error(w, "invalid Tg-Chat-Id header", http.StatusBadRequest)
		return
	}

	if !s.requireChat(w, r, chatID) {
		return
	}

	var req scrapDomain.RemoveLinkRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Link == "" {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	exists, err := s.repo.HasLink(r.Context(), chatID, req.Link)
	if err != nil {
		http.Error(w, "database error", 500)
		return
	}
	if !exists {
		http.Error(w, "subscription not found", 404)
		return
	}

	start := time.Now()
	err = s.repo.RemoveLink(r.Context(), chatID, req.Link)
	duration := time.Since(start).Milliseconds()
	metrics.RequestDuration.WithLabelValues("database", "links_table").Observe(float64(duration))

	if err != nil {
		s.logger.Error("Ошибка удаления ссылки", "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	_ = s.cache.Invalidate(r.Context(), chatID)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(scrapDomain.LinkResponse{ID: 0, URL: req.Link})
}

func getSourceFromURL(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "unknown"
	}
	return u.Host
}

func (s *ScrapperServer) requireChat(w http.ResponseWriter, r *http.Request, id int64) bool {
	exists, err := s.repo.ChatExists(r.Context(), id)
	if err != nil {
		http.Error(w, "database error", 500)
		return false
	}
	if !exists {
		http.Error(w, "chat not registered", 404)
		return false
	}
	return true
}
