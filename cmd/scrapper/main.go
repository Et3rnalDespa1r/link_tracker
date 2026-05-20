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
	"github.com/jackc/pgx/v5/pgxpool"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/config"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/infrastructure/repository/postgres"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/client"
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

	pool, err := pgxpool.New(ctx, cfg.DatabaseDSN())
	if err != nil {
		logger.Error("Ошибка подключения к БД", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	repo := postgres.NewSqlLinkRepository(pool)

	botClient := client.NewBotClient(cfg.BotURL)

	githubClient := client.NewGithubClient("https://api.github.com", cfg.GithubToken)
	soClient := client.NewStackOverflowClient("https://api.stackexchange.com/2.3")

	updateSvc := updater.NewUpdater(botClient, repo, cfg, logger, githubClient, soClient)

	scheduler := gocron.NewScheduler(time.UTC)

	_, err = scheduler.Every(cfg.CheckInterval).Do(func() {
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
