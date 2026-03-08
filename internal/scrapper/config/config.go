package config

import (
	"fmt"

	"github.com/caarlos0/env/v11"
	"github.com/joho/godotenv"
)

type Config struct {
	ServerAddress string `env:"SCRAPPER_ADDRESS" envDefault:":8081"`
	BotURL        string `env:"BOT_URL" envDefault:"http://localhost:8080"`
}

func Load() (*Config, error) {
	_ = godotenv.Load()

	var cfg Config
	err := env.Parse(&cfg)
	if err != nil {
		return nil, fmt.Errorf("config error: %w", err)
	}

	return &cfg, nil
}
