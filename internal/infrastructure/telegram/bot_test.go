package telegram

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRouteCommand(t *testing.T) {
	t.Run("start command", func(t *testing.T) {
		expected := "Добро пожаловать в LinkTracker! Используйте /help, чтобы посмотреть доступные команды."
		actual := RouteCommand("start")
		assert.Equal(t, expected, actual)
	})

	t.Run("help command", func(t *testing.T) {
		expected := "Доступные команды:\n/start - Начало работы\n/help - Список команд"
		actual := RouteCommand("help")
		assert.Equal(t, expected, actual)
	})

	t.Run("unknown command", func(t *testing.T) {
		expected := "Неизвестная команда. Воспользуйтесь /help, чтобы посмотреть список доступных команд."
		actual := RouteCommand("qwerty")
		assert.Equal(t, expected, actual)
	})
}
