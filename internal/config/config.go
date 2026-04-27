package config

import (
	"fmt"

	"github.com/caarlos0/env/v11"
	"github.com/joho/godotenv"
)

type Config struct {
	TelegramToken string `env:"APP_TELEGRAM_TOKEN,required"`
	BotTimeout    int    `env:"BOT_TIMEOUT" envDefault:"60"`
	BotAddress    string `env:"BOT_ADDRESS" envDefault:":8080"`

	ServerAddress string `env:"SCRAPPER_ADDRESS" envDefault:":8081"`
	BotURL        string `env:"BOT_URL" envDefault:"http://localhost:8080"`

	DBHost     string `env:"DB_HOST" envDefault:"localhost"`
	DBPort     string `env:"DB_PORT" envDefault:"5432"`
	DBUser     string `env:"DB_USER" envDefault:"postgres"`
	DBPassword string `env:"DB_PASSWORD,required"`
	DBName     string `env:"DB_NAME" envDefault:"linktracker"`
	AccessType string `env:"ACCESS_TYPE" envDefault:"SQL"`
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
