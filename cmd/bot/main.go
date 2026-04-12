package main

import (
	"context"
	"fmt"
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

const shutdownTimeout = 5 * time.Second

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	cfg, err := config.Load()
	if err != nil {
		logger.Error("Не удалось загрузить конфиг", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	repo := repository.NewInMemoryLinkRepository()

	tgBot, err := telegram.NewBot(cfg.TelegramToken, cfg.BotTimeout, logger)
	if err != nil {
		logger.Error("Ошибка инициализации Telegram-клиента", "error", err)
		os.Exit(1)
	}

	botService := application.NewBotService(repo, tgBot, logger)
	tgBot.SetService(botService)

	if err := tgBot.SetupCommands(); err != nil {
		logger.Error("Не удалось установить команды меню", "error", err)
	}

	httpServer := http.NewBotServer(cfg.BotAddress, botService, logger)

	errCh := make(chan error, 2)

	go func() {
		if err := httpServer.Start(); err != nil {
			errCh <- fmt.Errorf("ошибка HTTP сервера: %w", err)
		}
	}()

	go func() {
		if err := tgBot.Start(ctx); err != nil {
			errCh <- fmt.Errorf("ошибка Telegram бота: %w", err)
		}
	}()

	select {
	case <-ctx.Done():
		logger.Info("Остановка приложения по сигналу...")
	case err := <-errCh:
		logger.Error("Аварийная остановка приложения", "error", err)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	if err := httpServer.Stop(shutdownCtx); err != nil {
		logger.Error("Ошибка при остановке HTTP сервера", "error", err)
	}

	logger.Info("Приложение успешно остановлено")
}
