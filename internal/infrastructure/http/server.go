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
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/config"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/infrastructure/cache"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/metrics"
	"golang.org/x/time/rate"

	coreDomain "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/domain"
	scrapDomain "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/domain"
)

type LinkRepository interface {
	AddLink(ctx context.Context, chatID int64, link coreDomain.Link) error
	RemoveLink(ctx context.Context, chatID int64, url string) error
	GetLinks(ctx context.Context, chatID int64) ([]coreDomain.Link, error)
}

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
	repo   LinkRepository
}

func NewScrapperServer(cfg *config.Config, logger *slog.Logger, respCache *cache.ResponseCache, repo LinkRepository) *ScrapperServer {
	mux := http.NewServeMux()
	s := &ScrapperServer{
		logger: logger,
		cache:  respCache,
		repo:   repo,
	}

	mux.HandleFunc("POST /tg-chat/{id}", s.handleRegisterChat)
	mux.HandleFunc("DELETE /tg-chat/{id}", s.handleDeleteChat)
	mux.HandleFunc("POST /links", s.handleAddLink)
	mux.HandleFunc("DELETE /links", s.handleRemoveLink)
	mux.HandleFunc("GET /links", s.handleGetLinks)

	mux.Handle("GET /metrics", promhttp.Handler())

	limiter := NewIPRateLimiter(rate.Limit(cfg.RateLimitRPS), cfg.RateLimitBurst)
	handlerWithRateLimit := rateLimitMiddleware(limiter, mux)

	s.srv = &http.Server{
		Addr:    cfg.ServerAddress,
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
	metrics.ApiRequests.WithLabelValues("tg_bot").Inc()
	w.WriteHeader(http.StatusOK)
}

func (s *ScrapperServer) handleDeleteChat(w http.ResponseWriter, r *http.Request) {
	metrics.ApiRequests.WithLabelValues("tg_bot").Inc()
	w.WriteHeader(http.StatusOK)
}

func (s *ScrapperServer) handleGetLinks(w http.ResponseWriter, r *http.Request) {
	metrics.ApiRequests.WithLabelValues("tg_bot").Inc()

	chatID, _ := strconv.ParseInt(r.Header.Get("Tg-Chat-Id"), 10, 64)

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
	metrics.ApiRequests.WithLabelValues("tg_bot").Inc()

	chatID, _ := strconv.ParseInt(r.Header.Get("Tg-Chat-Id"), 10, 64)

	var req linkRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	start := time.Now()
	linkToSave := coreDomain.Link{URL: req.Link}
	err := s.repo.AddLink(r.Context(), chatID, linkToSave)
	duration := time.Since(start).Milliseconds()
	metrics.RequestDuration.WithLabelValues("database", "links_table").Observe(float64(duration))

	if err != nil {
		s.logger.Error("Ошибка добавления в БД", "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	source := getSourceFromURL(req.Link)
	metrics.LinksOnTrack.WithLabelValues(source).Inc()

	_ = s.cache.Invalidate(r.Context(), chatID)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(scrapDomain.LinkResponse{ID: 0, URL: req.Link})
}

func (s *ScrapperServer) handleRemoveLink(w http.ResponseWriter, r *http.Request) {
	metrics.ApiRequests.WithLabelValues("tg_bot").Inc()

	chatID, _ := strconv.ParseInt(r.Header.Get("Tg-Chat-Id"), 10, 64)

	var req linkRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	start := time.Now()
	err := s.repo.RemoveLink(r.Context(), chatID, req.Link)
	duration := time.Since(start).Milliseconds()
	metrics.RequestDuration.WithLabelValues("database", "links_table").Observe(float64(duration))

	if err != nil {
		s.logger.Error("Ошибка удаления из БД", "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	source := getSourceFromURL(req.Link)
	metrics.LinksOnTrack.WithLabelValues(source).Dec()

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
