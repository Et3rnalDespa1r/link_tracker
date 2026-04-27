package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-co-op/gocron"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/config"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/client"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/http"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/updater"
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

	botClient := client.NewBotClient(cfg.BotURL)
	updateSvc := updater.NewUpdater(botClient, logger)

	scheduler := gocron.NewScheduler(time.UTC)
	_, err = scheduler.Every(30).Seconds().Do(func() {
		updateSvc.ProcessUpdates(context.Background())
	})
	if err != nil {
		logger.Error("Ошибка настройки планировщика", slog.Any("error", err))
		os.Exit(1)
	}
	scheduler.StartAsync()

	httpServer := http.NewScrapperServer(cfg.ServerAddress, logger)

	go func() {
		if err := httpServer.Start(); err != nil {
			logger.Error("Ошибка работы HTTP сервера", slog.Any("error", err))
		}
	}()

	<-ctx.Done()

	scheduler.Stop()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := httpServer.Stop(shutdownCtx); err != nil {
		logger.Error("Ошибка при остановке HTTP сервера", slog.Any("error", err))
	}

	logger.Info("Приложение успешно остановлено")
}
