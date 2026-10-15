package httpapi

import (
	"context"

	"go.uber.org/zap"

	"github.com/Kasp3rR/segment-service-VK/internal/service"
)

type API struct {
	service *service.Service
	ready   func(context.Context) error
	logger  *zap.Logger
}
