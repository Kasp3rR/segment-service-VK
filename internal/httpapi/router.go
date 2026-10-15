package httpapi

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/Kasp3rR/segment-service-VK/internal/service"
)

func NewRouter(segmentService *service.Service, ready func(context.Context) error, logger *zap.Logger, timeout time.Duration) http.Handler {
	a := &API{
		service: segmentService,
		ready:   ready,
		logger:  logger,
	}
	router := gin.New()
	_ = router.SetTrustedProxies(nil)
	router.HandleMethodNotAllowed = true
	router.Use(requestMiddleware(logger, timeout))
	router.Use(recoveryMiddleware(logger))
	router.NoRoute(func(c *gin.Context) {
		writeError(c.Writer, http.StatusNotFound, "not_found", "route not found")
	})
	router.NoMethod(func(c *gin.Context) {
		writeError(c.Writer, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
	})
	router.GET("/healthz", func(c *gin.Context) {
		writeJSON(c.Writer, http.StatusOK, gin.H{"status": "ok"})
	})
	router.GET("/readyz", func(c *gin.Context) {
		if err := a.ready(c.Request.Context()); err != nil {
			writeError(c.Writer, http.StatusServiceUnavailable, "unavailable", "database unavailable")
			return
		}
		writeJSON(c.Writer, http.StatusOK, gin.H{"status": "ready"})
	})
	router.POST("/segments", a.createSegment)
	router.GET("/segments", a.listSegments)
	router.GET("/segments/:id", a.getSegment)
	router.PATCH("/segments/:id", a.patchSegment)
	router.DELETE("/segments/:id", a.deleteSegment)
	router.POST("/users", a.createUser)
	router.PUT("/users/:user_id/segments/:segment_id", a.addMembership)
	router.DELETE("/users/:user_id/segments/:segment_id", a.removeMembership)
	router.GET("/users/:user_id/segments", a.userSegments)
	router.POST("/segments/:id/distributions", a.startDistribution)
	router.GET("/distributions/:job_id", a.getDistribution)
	return router
}
