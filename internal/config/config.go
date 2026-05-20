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

	ServerAddress string `env:"SCRAPPER_ADDRESS" envDefault:":8083"`
	BotURL        string `env:"BOT_URL" envDefault:"http://localhost:8080"`

	DBHost     string `env:"DB_HOST" envDefault:"localhost"`
	DBPort     string `env:"DB_PORT" envDefault:"5432"`
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
	KafkaTopic   string   `env:"KAFKA_TOPIC" envDefault:"link.updates"`

	KafkaGroupID      string `env:"KAFKA_GROUP_ID" envDefault:"link-tracker-bot-group"`
	KafkaRetries      int    `env:"KAFKA_RETRIES" envDefault:"3"`
	SchemaRegistryURL string `env:"SCHEMA_REGISTRY_URL" envDefault:"http://localhost:8081"`
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
