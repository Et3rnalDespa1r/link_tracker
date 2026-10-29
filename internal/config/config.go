package config

import (
	"fmt"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/joho/godotenv"
)

type Config struct {
	TelegramToken string `env:"APP_TELEGRAM_TOKEN,required"`
	BotTimeout    int    `env:"BOT_TIMEOUT" envDefault:"60"`
	BotAddress    string `env:"BOT_ADDRESS" envDefault:":8080"`

	ServerAddress  string `env:"SCRAPPER_ADDRESS" envDefault:":8083"`
	ScrapperURL    string `env:"SCRAPPER_URL" envDefault:"http://localhost:8083"`
	PushgatewayURL string `env:"PUSHGATEWAY_URL" envDefault:""`
	BotURL         string `env:"BOT_URL" envDefault:"http://localhost:8080"`

	DBHost     string `env:"DB_HOST" envDefault:"localhost"`
	DBPort     string `env:"DB_PORT" envDefault:"5433"`
	DBUser     string `env:"DB_USER" envDefault:"postgres"`
	DBPassword string `env:"DB_PASSWORD,required"`
	DBName     string `env:"DB_NAME" envDefault:"linktracker"`
	AccessType string `env:"ACCESS_TYPE" envDefault:"SQL"`

	BatchSize     int           `env:"BATCH_SIZE" envDefault:"100"`
	WorkerCount   int           `env:"WORKER_COUNT" envDefault:"4"`
	CheckInterval time.Duration `env:"CHECK_INTERVAL" envDefault:"5m"`

	GithubToken string `env:"GITHUB_TOKEN"`

	UseKafka     bool     `env:"USE_KAFKA" envDefault:"true"`
	KafkaBrokers []string `env:"KAFKA_BROKERS" envSeparator:"," envDefault:"localhost:9092,localhost:9094,localhost:9095"`

	KafkaTopic string `env:"KAFKA_TOPIC" envDefault:"link.updates"`

	KafkaGroupID      string `env:"KAFKA_GROUP_ID" envDefault:"link-tracker-bot-group"`
	KafkaRetries      int    `env:"KAFKA_RETRIES" envDefault:"3"`
	SchemaRegistryURL string `env:"SCHEMA_REGISTRY_URL" envDefault:"http://localhost:8081"`

	ValkeyAddresses []string      `env:"VALKEY_ADDRESSES" envDefault:"localhost:6379,localhost:6380,localhost:6381"`
	ValkeyCacheTTL  time.Duration `env:"VALKEY_CACHE_TTL" envDefault:"5m"`

	HTTPTimeout time.Duration `env:"HTTP_TIMEOUT" envDefault:"5s"`

	RetryAttempts     int           `env:"RETRY_ATTEMPTS" envDefault:"3"`
	RetryDelay        time.Duration `env:"RETRY_DELAY" envDefault:"1s"`
	RetryableStatuses []int         `env:"RETRYABLE_STATUSES" envSeparator:"," envDefault:"500,502,503,504"`
	RetryExponential  bool          `env:"RETRY_EXPONENTIAL" envDefault:"false"`

	CBSlidingWindowSize        uint32        `env:"CB_SLIDING_WINDOW_SIZE" envDefault:"10"`
	CBMinimumRequiredCalls     uint32        `env:"CB_MINIMUM_REQUIRED_CALLS" envDefault:"5"`
	CBFailureRateThreshold     float64       `env:"CB_FAILURE_RATE_THRESHOLD" envDefault:"50.0"`
	CBPermittedCallsInHalfOpen uint32        `env:"CB_PERMITTED_CALLS_IN_HALF_OPEN" envDefault:"5"`
	CBWaitDurationInOpenState  time.Duration `env:"CB_WAIT_DURATION_IN_OPEN_STATE" envDefault:"10s"`

	RateLimitRPS   float64 `env:"RATE_LIMIT_RPS" envDefault:"10.0"`
	RateLimitBurst int     `env:"RATE_LIMIT_BURST" envDefault:"20"`

	AgentHFToken        string `env:"AGENT_HF_TOKEN" envDefault:""`
	AgentRawTopic       string `env:"AGENT_RAW_TOPIC" envDefault:"link.raw-updates"`
	AgentProcessedTopic string `env:"AGENT_PROCESSED_TOPIC" envDefault:"link.processed-updates"`
	AgentGroupID        string `env:"AGENT_GROUP_ID" envDefault:"link-tracker-agent-group"`

	AgentStopWords        []string      `env:"AGENT_STOP_WORDS" envSeparator:"," envDefault:"spam,ads,promo"`
	AgentExcludedAuthors  []string      `env:"AGENT_EXCLUDED_AUTHORS" envSeparator:"," envDefault:"bot-user,spammer"`
	AgentMinLength        int           `env:"AGENT_MIN_LENGTH" envDefault:"20"`
	AgentSummaryThreshold int           `env:"AGENT_SUMMARY_THRESHOLD" envDefault:"500"`
	AgentHighKeywords     []string      `env:"AGENT_HIGH_KEYWORDS" envSeparator:"," envDefault:"critical,urgent,breaking,security"`
	AgentLowKeywords      []string      `env:"AGENT_LOW_KEYWORDS" envSeparator:"," envDefault:"minor,typo,chore,docs"`
	AgentGroupWindow      time.Duration `env:"AGENT_GROUP_WINDOW" envDefault:"30s"`
}

func (c *Config) DatabaseDSN() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		c.DBUser, c.DBPassword, c.DBHost, c.DBPort, c.DBName)
}

func Load() (*Config, error) {
	_ = godotenv.Load()

	var cfg Config
	err := env.Parse(&cfg)
	if err != nil {
		return nil, fmt.Errorf("ошибка загрузки конфига: %w", err)
	}

	return &cfg, nil
}
