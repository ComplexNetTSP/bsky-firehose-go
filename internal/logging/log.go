package logging

import (
	"context"
	"log/slog"
	"os"
)

func NewLogger(logLevel string) *slog.Logger {
	var level slog.Level
	switch logLevel {
	case "DEBUG":
		level = slog.LevelDebug
	case "INFO":
		level = slog.LevelInfo
	case "WARN":
		level = slog.LevelWarn
	default:
		level = slog.LevelInfo
	}
	handler := slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: level})
	logger := slog.New(handler)
	return logger
}

func ContextWithLogger(ctx context.Context, logger *slog.Logger) context.Context {
	return context.WithValue(ctx, "logger", logger)
}

func LoggerFromContext(ctx context.Context) *slog.Logger {
	if logger, ok := ctx.Value("logger").(*slog.Logger); ok {
		return logger
	}
	return slog.Default()
}
