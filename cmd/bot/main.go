package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/application"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/infrastructure/config"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/infrastructure/http"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/infrastructure/repository"
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

	repo := repository.NewInMemoryLinkRepository()

	tgBot, err := telegram.NewBot(cfg.TelegramToken, cfg.BotTimeout, logger)
	if err != nil {
		logger.Error("Ошибка инициализации Telegram-клиента", slog.Any("error", err))
		os.Exit(1)
	}

	botService := application.NewBotService(repo, tgBot, logger)
	tgBot.SetService(botService)

	if err := tgBot.SetupCommands(); err != nil {
		logger.Error("Не удалось установить команды меню", slog.Any("error", err))
	}

	httpServer := http.NewBotServer(cfg.BotAddress, botService, logger)

	go func() {
		if err := httpServer.Start(); err != nil {
			logger.Error("Ошибка работы HTTP сервера", slog.Any("error", err))
		}
	}()

	go tgBot.Start(ctx)

	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := httpServer.Stop(shutdownCtx); err != nil {
		logger.Error("Ошибка при остановке HTTP сервера", slog.Any("error", err))
	}

	logger.Info("Приложение успешно остановлено")
}
