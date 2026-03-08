package config

import (
	"fmt"

	"github.com/caarlos0/env/v11"
	"github.com/joho/godotenv"
)

type Config struct {
	TelegramToken string `env:"APP_TELEGRAM_TOKEN,required"`
	BotTimeout    int    `env:"BOT_TIMEOUT" envDefault:"60"`
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
