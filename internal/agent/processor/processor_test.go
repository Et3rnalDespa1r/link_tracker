package processor

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestProcessor_ProcessUpdate(t *testing.T) {
	stopWords := []string{"spam", "реклама"}
	excludedAuthors := []string{"spambot", "bad_user"}
	minLength := 10
	summaryThreshold := 20

	proc := NewProcessor(stopWords, excludedAuthors, minLength, summaryThreshold, "")

	t.Run("TC-2.3: Фильтрация по минимальной длине (Слишком короткое)", func(t *testing.T) {
		raw := RawUpdate{
			ID:          1,
			Description: "Коротко",
			Author:      "good_user",
		}
		result := proc.ProcessUpdate(raw)
		assert.Nil(t, result)
	})

	t.Run("TC-2.2: Фильтрация по авторам (Исключенный автор)", func(t *testing.T) {
		raw := RawUpdate{
			ID:          2,
			Description: "Длинное нормальное сообщение",
			Author:      "SpaMBoT",
		}
		result := proc.ProcessUpdate(raw)
		assert.Nil(t, result)
	})

	t.Run("TC-2.1: Фильтрация по стоп-словам", func(t *testing.T) {
		raw := RawUpdate{
			ID:          3,
			Description: "Тут какая-то Реклама продукта",
			Author:      "good_user",
		}
		result := proc.ProcessUpdate(raw)
		assert.Nil(t, result)
	})

	t.Run("TC-2.4 и TC-3.2: Пропуск валидного и Короткий текст (Без суммаризации)", func(t *testing.T) {
		raw := RawUpdate{
			ID:          4,
			Description: "Нормальный текст",
			Author:      "good_user",
			TgChatIDs:   []int64{123, 456},
		}
		result := proc.ProcessUpdate(raw)

		assert.NotNil(t, result)
		assert.Equal(t, raw.Description, result.Description)
		assert.Equal(t, "HIGH", result.Priority)
		assert.ElementsMatch(t, raw.TgChatIDs, result.TgChatIDs)
	})

	t.Run("TC-3.1: Суммаризация длинного текста", func(t *testing.T) {
		raw := RawUpdate{
			ID:          5,
			Description: "Это очень длинный текст, который должен быть обрезан",
			Author:      "good_user",
		}
		result := proc.ProcessUpdate(raw)

		assert.NotNil(t, result)
		expected := string([]rune(raw.Description)[:summaryThreshold]) + "..."
		assert.Equal(t, expected, result.Description)
		assert.Equal(t, "HIGH", result.Priority)
	})
}
