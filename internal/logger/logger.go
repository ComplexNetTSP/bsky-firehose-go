package logger

import (
	"log/slog"
	"os"
	"sync"
	"time"
)

var (
	log  *slog.Logger
	once sync.Once
	loc  *time.Location
)

func GetLogger() *slog.Logger {
	once.Do(func() {
		var err error
		loc, err = time.LoadLocation("Europe/Paris")
		if err != nil {
			panic(err)
		}

		handler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
				if a.Key == slog.TimeKey {
					if t, ok := a.Value.Any().(time.Time); ok {
						a.Value = slog.TimeValue(t.In(loc))
					}
				}
				return a
			},
		})

		log = slog.New(handler)
	})

	return log
}
