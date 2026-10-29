package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"net/http"
	"strconv"
	"time"
)

var HTTPRequests = promauto.NewCounterVec(prometheus.CounterOpts{Name: "linktracker_http_requests_total", Help: "HTTP requests by status."}, []string{"method", "route", "status"})
var HTTPDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{Name: "linktracker_http_duration_seconds", Help: "HTTP request duration.", Buckets: prometheus.DefBuckets}, []string{"method", "route"})

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(status int) {
	if w.status != 0 {
		return
	}
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}
func (w *statusWriter) Write(data []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(200)
	}
	return w.ResponseWriter.Write(data)
}
func Instrument(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics" {
			next.ServeHTTP(w, r)
			return
		}
		started := time.Now()
		out := &statusWriter{ResponseWriter: w}
		next.ServeHTTP(out, r)
		route := r.Pattern
		if route == "" {
			route = "unmatched"
		}
		status := out.status
		if status == 0 {
			status = 200
		}
		HTTPRequests.WithLabelValues(r.Method, route, strconv.Itoa(status)).Inc()
		HTTPDuration.WithLabelValues(r.Method, route).Observe(time.Since(started).Seconds())
	})
}
