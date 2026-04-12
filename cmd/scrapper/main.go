package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-co-op/gocron"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/client"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/config"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/http"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/updater"
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

	botClient := client.NewBotClient(cfg.BotURL)
	updateSvc := updater.NewUpdater(botClient, logger)

	scheduler := gocron.NewScheduler(time.UTC)
	_, err = scheduler.Every(30).Seconds().Do(func() {
		updateSvc.ProcessUpdates(ctx)
	})
	if err != nil {
		logger.Error("Ошибка настройки планировщика", "error", err)
		os.Exit(1)
	}
	scheduler.StartAsync()

	httpServer := http.NewScrapperServer(cfg.ServerAddress, logger)

	errCh := make(chan error, 1)

	go func() {
		if err := httpServer.Start(); err != nil {
			errCh <- fmt.Errorf("ошибка HTTP сервера: %w", err)
		}
	}()

	select {
	case <-ctx.Done():
		logger.Info("Остановка скраппера по сигналу...")
	case err := <-errCh:
		logger.Error("Аварийная остановка скраппера", "error", err)
	}

	scheduler.Stop()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	if err := httpServer.Stop(shutdownCtx); err != nil {
		logger.Error("Ошибка при остановке HTTP сервера", "error", err)
	}

	logger.Info("Приложение успешно остановлено")
}
