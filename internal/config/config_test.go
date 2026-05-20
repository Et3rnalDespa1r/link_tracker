package config_test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/config"
)

func TestConfig_SenderTypeSelection(t *testing.T) {
	tests := []struct {
		name           string
		envKafkaValue  string
		expectedResult bool
	}{
		{
			name:           "Должна выбраться Kafka, если USE_KAFKA=true",
			envKafkaValue:  "true",
			expectedResult: true,
		},
		{
			name:           "Должен выбраться HTTP, если USE_KAFKA=false",
			envKafkaValue:  "false",
			expectedResult: false,
		},
		{
			name:           "По умолчанию (без env) должна выбираться Kafka (true)",
			envKafkaValue:  "",
			expectedResult: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			os.Clearenv()

			os.Setenv("APP_TELEGRAM_TOKEN", "fake_token")
			os.Setenv("DB_PASSWORD", "fake_pass")

			if tt.envKafkaValue != "" {
				os.Setenv("USE_KAFKA", tt.envKafkaValue)
			}
			cfg, err := config.Load()
			require.NoError(t, err)

			assert.Equal(t, tt.expectedResult, cfg.UseKafka)
		})
	}
}
