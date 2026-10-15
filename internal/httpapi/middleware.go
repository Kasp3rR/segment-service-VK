package httpapi

import (
	"context"
	"io"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

func requestMiddleware(logger *zap.Logger, timeout time.Duration) gin.HandlerFunc {
	return func(c *gin.Context) {
		started := time.Now()
		ctx, cancel := context.WithTimeout(c.Request.Context(), timeout)
		defer cancel()
		c.Request = c.Request.WithContext(ctx)
		c.Next()
		logger.Info("http request",
			zap.String("method", c.Request.Method),
			zap.String("path", c.Request.URL.Path),
			zap.Int("status", c.Writer.Status()),
			zap.Duration("duration", time.Since(started)),
		)
	}
}

func recoveryMiddleware(logger *zap.Logger) gin.HandlerFunc {
	return gin.CustomRecoveryWithWriter(io.Discard, func(c *gin.Context, recovered any) {
		logger.Error("http panic", zap.Any("panic", recovered))
		c.Abort()
		writeError(c.Writer, http.StatusInternalServerError, "internal_error", "internal server error")
	})
}
