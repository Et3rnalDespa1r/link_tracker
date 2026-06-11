package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var LinksOnTrack = promauto.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "links_on_track_total",
		Help: "Количество ссылок в БД, поставленных на мониторинг",
	},
	[]string{"tracked_source"},
)

var RequestDuration = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "request_duration_ms_total",
		Help:    "Длительность одной операции (БД, внешние источники, Kafka) в миллисекундах",
		Buckets: []float64{10, 50, 100, 250, 500, 1000, 2500, 5000},
	},
	[]string{"scope", "scope_type"},
)

var ApiRequests = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "api_requests_total",
		Help: "Счетчик пришедших к API запросов",
	},
	[]string{"source"},
)

var CommandRequests = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "command_requests_total",
		Help: "Счетчик обработанных команд в боте",
	},
	[]string{"command"},
)

var BotCommandDuration = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "command_duration_ms_total",
		Help:    "Длительность одной операции в боте в миллисекундах",
		Buckets: []float64{10, 50, 100, 250, 500, 1000, 2500, 5000},
	},
	[]string{"scope", "scope_type"},
)

var SentNotifications = promauto.NewCounter(
	prometheus.CounterOpts{
		Name: "sent_notification_total",
		Help: "Счетчик отправленных нотификаций",
	},
)
