package httpapi

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/Kasp3rR/segment-service-VK/internal/domain"
)

func TestDecode(t *testing.T) {
	for _, tc := range []struct {
		name, body, contentType string
		status                  int
	}{{"object", `{"value":"yes"}`, "application/json", 200}, {"charset", `{}`, "application/json; charset=utf-8", 200}, {"empty", "", "application/json", 400}, {"malformed", "{", "application/json", 400}, {"unknown", `{"unknown":1}`, "application/json", 400}, {"trailing", `{} {}`, "application/json", 400}, {"array", `[]`, "application/json", 400}, {"wrong_type", `{"value":12}`, "application/json", 400}, {"wrong_media", `{}`, "text/plain", 415}, {"missing_media", `{}`, "", 415}, {"oversized", `{"value":"` + strings.Repeat("a", 65536) + `"}`, "application/json", 413}} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest("POST", "/", strings.NewReader(tc.body))
			r.Header.Set("Content-Type", tc.contentType)
			w := httptest.NewRecorder()
			var v struct {
				Value string `json:"value"`
			}
			ok := decode(w, r, &v)
			if w.Code != tc.status || ok != (tc.status == 200) {
				t.Fatalf("status=%d ok=%v body=%s", w.Code, ok, w.Body.String())
			}
		})
	}
}
func TestOptionalString(t *testing.T) {
	for _, tc := range []struct {
		input string
		valid bool
	}{{`""`, true}, {`"name"`, true}, {`null`, false}, {`123`, false}} {
		t.Run(tc.input, func(t *testing.T) {
			var v optionalString
			err := v.UnmarshalJSON([]byte(tc.input))
			if (err == nil) != tc.valid {
				t.Fatal(err)
			}
		})
	}
}
func TestErrorMapping(t *testing.T) {
	for _, tc := range []struct {
		name   string
		err    error
		status int
	}{{"validation", domain.ErrInvalid, 400}, {"missing", domain.ErrNotFound, 404}, {"conflict", domain.ErrConflict, 409}, {"timeout", context.DeadlineExceeded, 504}, {"internal", errors.New("password=secret"), 500}} {
		t.Run(tc.name, func(t *testing.T) {
			a := &API{logger: zap.NewNop()}
			w := httptest.NewRecorder()
			a.fail(w, httptest.NewRequest("GET", "/", nil), tc.err)
			if w.Code != tc.status || strings.Contains(w.Body.String(), "password") {
				t.Fatal(w.Body.String())
			}
		})
	}
}
func TestHealthAndReadiness(t *testing.T) {
	for _, tc := range []struct {
		name, path string
		dbErr      error
		status     int
	}{{"live", "/healthz", errors.New("offline"), 200}, {"ready", "/readyz", nil, 200}, {"unavailable", "/readyz", errors.New("offline"), 503}} {
		t.Run(tc.name, func(t *testing.T) {
			h := NewRouter(nil, func(context.Context) error { return tc.dbErr }, zap.NewNop(), time.Second)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, httptest.NewRequest("GET", tc.path, nil))
			if w.Code != tc.status {
				t.Fatal(w.Code)
			}
		})
	}
}

func TestOversizedTrailingBody(t *testing.T) {
	r := httptest.NewRequest("POST", "/", strings.NewReader(`{}`+strings.Repeat(" ", 65536)))
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	var v struct{}
	if decode(w, r, &v) || w.Code != 413 {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
}
func TestGinRecovery(t *testing.T) {
	h := NewRouter(nil, func(context.Context) error { panic("unexpected failure") }, zap.NewNop(), time.Second)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/readyz", nil))
	if w.Code != 500 || strings.Contains(w.Body.String(), "unexpected failure") {
		t.Fatalf("%d %s", w.Code, w.Body.String())
	}
}
func TestGinNotFoundAndMethod(t *testing.T) {
	h := NewRouter(nil, func(context.Context) error { return nil }, zap.NewNop(), time.Second)
	for _, tc := range []struct {
		method, path string
		status       int
	}{{"GET", "/unknown", 404}, {"POST", "/healthz", 405}} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(tc.method, tc.path, nil))
		if w.Code != tc.status || !strings.Contains(w.Body.String(), `"error"`) {
			t.Fatalf("%d %s", w.Code, w.Body.String())
		}
	}
}
