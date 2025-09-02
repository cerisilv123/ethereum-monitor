package service

import (
	"log/slog"
)

type Service struct {
	Logger  *slog.Logger
	Scanner *Scanner
}

func NewService(scanner *Scanner, logger *slog.Logger) *Service {
	return &Service{
		Logger:  logger,
		Scanner: scanner,
	}
}
