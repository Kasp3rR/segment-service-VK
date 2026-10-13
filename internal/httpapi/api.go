package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/Kasp3rR/segment-service-VK/internal/domain"
	"github.com/Kasp3rR/segment-service-VK/internal/service"
)

type API struct {
	service *service.Service
	ready   func(context.Context) error
	logger  *zap.Logger
}

func New(s *service.Service, ready func(context.Context) error, logger *zap.Logger, timeout time.Duration) http.Handler {
	a := &API{service: s, ready: ready, logger: logger}
	router := gin.New()
	_ = router.SetTrustedProxies(nil)
	router.HandleMethodNotAllowed = true
	router.Use(func(c *gin.Context) {
		started := time.Now()
		ctx, cancel := context.WithTimeout(c.Request.Context(), timeout)
		defer cancel()
		c.Request = c.Request.WithContext(ctx)
		c.Next()
		logger.Info("http request", zap.String("method", c.Request.Method), zap.String("path", c.Request.URL.Path), zap.Int("status", c.Writer.Status()), zap.Duration("duration", time.Since(started)))
	})
	router.Use(gin.CustomRecoveryWithWriter(io.Discard, func(c *gin.Context, recovered any) {
		logger.Error("http panic", zap.Any("panic", recovered))
		c.Abort()
		writeError(c.Writer, 500, "internal_error", "internal server error")
	}))
	router.NoRoute(func(c *gin.Context) { writeError(c.Writer, 404, "not_found", "route not found") })
	router.NoMethod(func(c *gin.Context) { writeError(c.Writer, 405, "method_not_allowed", "method not allowed") })
	router.GET("/healthz", func(c *gin.Context) { writeJSON(c.Writer, 200, gin.H{"status": "ok"}) })
	router.GET("/readyz", func(c *gin.Context) {
		if err := a.ready(c.Request.Context()); err != nil {
			writeError(c.Writer, 503, "unavailable", "database unavailable")
			return
		}
		writeJSON(c.Writer, 200, gin.H{"status": "ready"})
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
	return router
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]any{"error": map[string]string{"code": code, "message": message}})
}
func (a *API) fail(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, domain.ErrInvalid):
		writeError(w, 400, "invalid_input", err.Error())
	case errors.Is(err, domain.ErrNotFound):
		writeError(w, 404, "not_found", "resource not found")
	case errors.Is(err, domain.ErrConflict):
		writeError(w, 409, "conflict", "resource already exists")
	case errors.Is(err, context.DeadlineExceeded):
		writeError(w, 504, "timeout", "request deadline exceeded")
	default:
		a.logger.Error("request failed", zap.String("method", r.Method), zap.String("path", r.URL.Path), zap.Error(err))
		writeError(w, 500, "internal_error", "internal server error")
	}
}
func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || media != "application/json" {
		writeError(w, 415, "unsupported_media_type", "Content-Type must be application/json")
		return false
	}
	d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10))
	d.DisallowUnknownFields()
	if err = d.Decode(v); err == nil {
		var extra any
		if e := d.Decode(&extra); e != io.EOF {
			if e != nil {
				err = e
			} else {
				err = fmt.Errorf("exactly one JSON object is required")
			}
		}
	}
	if err != nil {
		var size *http.MaxBytesError
		if errors.As(err, &size) {
			writeError(w, 413, "body_too_large", "body must not exceed 64 KiB")
		} else {
			writeError(w, 400, "invalid_json", "invalid JSON object or unsupported field")
		}
		return false
	}
	return true
}
func pathID(c *gin.Context, key string) (int64, error) {
	id, err := strconv.ParseInt(c.Param(key), 10, 64)
	if err != nil || id <= 0 {
		return 0, fmt.Errorf("%w: %s must be a positive integer", domain.ErrInvalid, key)
	}
	return id, nil
}

type optionalString struct{ value *string }

func (s *optionalString) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		return errors.New("null is not supported")
	}
	var v string
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}
	s.value = &v
	return nil
}
func (a *API) createSegment(c *gin.Context) {
	w, r := c.Writer, c.Request
	var in *struct {
		Code        string `json:"code"`
		Name        string `json:"name"`
		Description string `json:"description"`
	}
	if !decode(w, r, &in) {
		return
	}
	if in == nil {
		a.fail(w, r, domain.ErrInvalid)
		return
	}
	s, err := a.service.CreateSegment(r.Context(), domain.CreateSegment{Code: in.Code, Name: in.Name, Description: in.Description})
	if err != nil {
		a.fail(w, r, err)
		return
	}
	w.Header().Set("Location", fmt.Sprintf("/segments/%d", s.ID))
	writeJSON(w, 201, s)
}
func (a *API) listSegments(c *gin.Context) {
	w, r := c.Writer, c.Request
	p := domain.Page{Limit: 100}
	var err error
	if v := r.URL.Query().Get("limit"); v != "" {
		p.Limit, err = strconv.Atoi(v)
		if err != nil {
			a.fail(w, r, domain.ErrInvalid)
			return
		}
	}
	if v := r.URL.Query().Get("after_id"); v != "" {
		p.AfterID, err = strconv.ParseInt(v, 10, 64)
		if err != nil {
			a.fail(w, r, domain.ErrInvalid)
			return
		}
	}
	out, err := a.service.ListSegments(r.Context(), p)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, 200, out)
}
func (a *API) getSegment(c *gin.Context) {
	w, r := c.Writer, c.Request
	id, err := pathID(c, "id")
	if err != nil {
		a.fail(w, r, err)
		return
	}
	s, err := a.service.GetSegment(r.Context(), id)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, 200, s)
}
func (a *API) patchSegment(c *gin.Context) {
	w, r := c.Writer, c.Request
	id, err := pathID(c, "id")
	if err != nil {
		a.fail(w, r, err)
		return
	}
	var in *struct {
		Name        optionalString `json:"name"`
		Description optionalString `json:"description"`
	}
	if !decode(w, r, &in) {
		return
	}
	if in == nil {
		a.fail(w, r, domain.ErrInvalid)
		return
	}
	s, err := a.service.PatchSegment(r.Context(), id, domain.PatchSegment{Name: in.Name.value, Description: in.Description.value})
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, 200, s)
}
func (a *API) deleteSegment(c *gin.Context) {
	w, r := c.Writer, c.Request
	id, err := pathID(c, "id")
	if err == nil {
		err = a.service.DeleteSegment(r.Context(), id)
	}
	if err != nil {
		a.fail(w, r, err)
		return
	}
	w.WriteHeader(204)
}
func (a *API) createUser(c *gin.Context) {
	w, r := c.Writer, c.Request
	var in *struct {
		ID int64 `json:"user_id"`
	}
	if !decode(w, r, &in) {
		return
	}
	if in == nil {
		a.fail(w, r, domain.ErrInvalid)
		return
	}
	u, err := a.service.CreateUser(r.Context(), in.ID)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, 201, u)
}
func (a *API) membership(c *gin.Context, add bool) {
	w, r := c.Writer, c.Request
	uid, err := pathID(c, "user_id")
	if err != nil {
		a.fail(w, r, err)
		return
	}
	sid, err := pathID(c, "segment_id")
	if err == nil {
		err = a.service.SetMembership(r.Context(), uid, sid, add)
	}
	if err != nil {
		a.fail(w, r, err)
		return
	}
	w.WriteHeader(204)
}
func (a *API) addMembership(c *gin.Context)    { a.membership(c, true) }
func (a *API) removeMembership(c *gin.Context) { a.membership(c, false) }
func (a *API) userSegments(c *gin.Context) {
	w, r := c.Writer, c.Request
	uid, err := pathID(c, "user_id")
	if err != nil {
		a.fail(w, r, err)
		return
	}
	out, err := a.service.UserSegments(r.Context(), uid)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, 200, out)
}
