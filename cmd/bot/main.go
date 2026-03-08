package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/infrastructure/config"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/infrastructure/telegram"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	cfg, err := config.Load()
	if err != nil {
		logger.Error("Не удалось загрузить конфиг", slog.Any("error", err))
		os.Exit(1)
	}
	
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	tgBot, err := telegram.NewBot(cfg.TelegramToken, cfg.BotTimeout, logger)
	if err != nil {
		logger.Error("Ошибка инициализации бота", slog.Any("error", err))
		os.Exit(1)
	}

	logger.Info("Бот успешно инициализирован")

	if err := tgBot.SetupCommands(); err != nil {
		logger.Error("Не удалось установить команды меню", slog.Any("error", err))
	}

	tgBot.Start(ctx)

	logger.Info("Приложение успешно остановлено")
}
