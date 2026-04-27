package db

import (
	"errors"
	"fmt"
	"log/slog"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

const migrationsPath = "file://migrations"

func RunMigrations(dsn string, logger *slog.Logger) error {
	logger.Info("Запуск миграций базы данных...")

	m, err := migrate.New(migrationsPath, dsn)
	if err != nil {

		return fmt.Errorf("ошибка инициализации миграций: %w", err)
	}
	defer m.Close()

	if err := m.Up(); err != nil {
		if errors.Is(err, migrate.ErrNoChange) {
			logger.Info("База данных уже в актуальном состоянии (миграции не требуются)")
			return nil
		}
		return fmt.Errorf("ошибка применения миграций: %w", err)
	}

	logger.Info("Миграции успешно применены!")
	return nil
}
