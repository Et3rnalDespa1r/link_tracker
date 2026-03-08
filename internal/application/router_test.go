package application

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRouteCommand(t *testing.T) {
	tests := []struct {
		name     string
		command  string
		expected string
	}{
		{
			name:     "happy path: start command",
			command:  CmdStart,
			expected: ReplyStart,
		},
		{
			name:     "happy path: help command",
			command:  CmdHelp,
			expected: ReplyHelp,
		},
		{
			name:     "edge case: unknown command",
			command:  "qwerty",
			expected: ReplyUnknown,
		},
		{
			name:     "edge case: empty command",
			command:  "",
			expected: ReplyUnknown,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := RouteCommand(tt.command)
			assert.Equal(t, tt.expected, actual)
		})
	}
}
