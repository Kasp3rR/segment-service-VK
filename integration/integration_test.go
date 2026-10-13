//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"

	"github.com/Kasp3rR/segment-service-VK/internal/domain"
	"github.com/Kasp3rR/segment-service-VK/internal/httpapi"
	"github.com/Kasp3rR/segment-service-VK/internal/postgres"
	"github.com/Kasp3rR/segment-service-VK/internal/service"
)

var sequence atomic.Int64

func fixture(t *testing.T, migrate bool) *postgres.Store {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Fatal("TEST_DATABASE_URL is required: integration tests use real PostgreSQL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("stage2_test_%d_%d", time.Now().UnixNano(), sequence.Add(1))
	quoted := pgx.Identifier{schema}.Sanitize()
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+quoted); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, err := admin.Exec(ctx, "DROP SCHEMA "+quoted+" CASCADE")
		admin.Close()
		if err != nil {
			t.Errorf("cleanup schema: %v", err)
		}
	})
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	cfg.MaxConns = 10
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	s := &postgres.Store{Pool: pool}
	if migrate {
		if err = postgres.Migrate(ctx, pool, "up"); err != nil {
			t.Fatal(err)
		}
	}
	return s
}
func createSegment(t *testing.T, s *postgres.Store, code string) domain.Segment {
	t.Helper()
	v, err := s.CreateSegment(context.Background(), domain.CreateSegment{Code: code, Name: "Name", Description: "Initial"})
	if err != nil {
		t.Fatal(err)
	}
	return v
}
func createUser(t *testing.T, s *postgres.Store, id int64) {
	t.Helper()
	if _, err := s.CreateUser(context.Background(), id); err != nil {
		t.Fatal(err)
	}
}
func str(v string) *string { return &v }
func TestSegmentCRUD(t *testing.T) {
	s := fixture(t, true)
	ctx := context.Background()
	a := createSegment(t, s, "MAIL_GPT")
	b := createSegment(t, s, "CLOUD")
	got, err := s.GetSegment(ctx, a.ID)
	if err != nil || got.Code != a.Code || got.CreatedAt.IsZero() {
		t.Fatalf("%+v %v", got, err)
	}
	list, err := s.ListSegments(ctx, domain.Page{Limit: 1})
	if err != nil || len(list) != 1 || list[0].ID != a.ID {
		t.Fatalf("%+v %v", list, err)
	}
	list, err = s.ListSegments(ctx, domain.Page{Limit: 1, AfterID: a.ID})
	if err != nil || len(list) != 1 || list[0].ID != b.ID {
		t.Fatal("pagination")
	}
	got, err = s.PatchSegment(ctx, a.ID, domain.PatchSegment{Name: str("Updated"), Description: str("")})
	if err != nil || got.Code != "MAIL_GPT" || got.Name != "Updated" || got.Description != "" {
		t.Fatalf("%+v %v", got, err)
	}
	if err = s.DeleteSegment(ctx, a.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = s.GetSegment(ctx, a.ID); !errors.Is(err, domain.ErrNotFound) {
		t.Fatal(err)
	}
}
func TestUniqueSegmentCode(t *testing.T) {
	s := fixture(t, true)
	createSegment(t, s, "MAIL_GPT")
	_, err := s.CreateSegment(context.Background(), domain.CreateSegment{Code: "MAIL_GPT", Name: "Other"})
	if !errors.Is(err, domain.ErrConflict) {
		t.Fatal(err)
	}
}
func TestCodeImmutableInDatabase(t *testing.T) {
	s := fixture(t, true)
	v := createSegment(t, s, "A")
	if _, err := s.Pool.Exec(context.Background(), `UPDATE segments SET code='B' WHERE id=$1`, v.ID); err == nil {
		t.Fatal("database allowed code change")
	}
}
func TestCreateUserAndDuplicate(t *testing.T) {
	s := fixture(t, true)
	u, err := s.CreateUser(context.Background(), 9223372036854775807)
	if err != nil || u.ID != 9223372036854775807 || u.CreatedAt.IsZero() {
		t.Fatalf("%+v %v", u, err)
	}
	_, err = s.CreateUser(context.Background(), u.ID)
	if !errors.Is(err, domain.ErrConflict) {
		t.Fatal(err)
	}
}
func TestMembershipIdempotency(t *testing.T) {
	s := fixture(t, true)
	ctx := context.Background()
	v := createSegment(t, s, "A")
	createUser(t, s, 42)
	for i := 0; i < 2; i++ {
		if err := s.SetMembership(ctx, 42, v.ID, true); err != nil {
			t.Fatal(err)
		}
	}
	list, err := s.UserSegments(ctx, 42)
	if err != nil || len(list) != 1 || list[0].Code != "A" {
		t.Fatalf("%+v %v", list, err)
	}
	for i := 0; i < 2; i++ {
		if err = s.SetMembership(ctx, 42, v.ID, false); err != nil {
			t.Fatal(err)
		}
	}
	list, err = s.UserSegments(ctx, 42)
	if err != nil || list == nil || len(list) != 0 {
		t.Fatalf("%+v %v", list, err)
	}
}
func TestMissingResources(t *testing.T) {
	s := fixture(t, true)
	ctx := context.Background()
	v := createSegment(t, s, "A")
	createUser(t, s, 42)
	for _, tc := range []struct {
		name string
		call func() error
	}{{"get_segment", func() error { _, e := s.GetSegment(ctx, 999); return e }}, {"patch_segment", func() error { _, e := s.PatchSegment(ctx, 999, domain.PatchSegment{Name: str("x")}); return e }}, {"delete_segment", func() error { return s.DeleteSegment(ctx, 999) }}, {"missing_user_add", func() error { return s.SetMembership(ctx, 999, v.ID, true) }}, {"missing_segment_add", func() error { return s.SetMembership(ctx, 42, 999, true) }}, {"missing_user_remove", func() error { return s.SetMembership(ctx, 999, v.ID, false) }}, {"missing_segment_remove", func() error { return s.SetMembership(ctx, 42, 999, false) }}, {"user_segments", func() error { _, e := s.UserSegments(ctx, 999); return e }}} {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.call(); !errors.Is(err, domain.ErrNotFound) {
				t.Fatal(err)
			}
		})
	}
}
func TestDeleteSegmentCascades(t *testing.T) {
	s := fixture(t, true)
	ctx := context.Background()
	v := createSegment(t, s, "A")
	createUser(t, s, 42)
	if err := s.SetMembership(ctx, 42, v.ID, true); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteSegment(ctx, v.ID); err != nil {
		t.Fatal(err)
	}
	list, err := s.UserSegments(ctx, 42)
	if err != nil || len(list) != 0 {
		t.Fatal("cascade failed")
	}
}
func TestUserSegmentsSortedAndIsolated(t *testing.T) {
	s := fixture(t, true)
	ctx := context.Background()
	a := createSegment(t, s, "A")
	b := createSegment(t, s, "B")
	createUser(t, s, 1)
	createUser(t, s, 2)
	for _, id := range []int64{b.ID, a.ID} {
		if err := s.SetMembership(ctx, 1, id, true); err != nil {
			t.Fatal(err)
		}
	}
	list, err := s.UserSegments(ctx, 1)
	if err != nil || len(list) != 2 || list[0].ID != a.ID || list[1].ID != b.ID {
		t.Fatal("wrong ordering")
	}
	other, err := s.UserSegments(ctx, 2)
	if err != nil || len(other) != 0 {
		t.Fatal("membership leaked between users")
	}
}
func TestConcurrentMemberships(t *testing.T) {
	s := fixture(t, true)
	ctx := context.Background()
	v := createSegment(t, s, "A")
	createUser(t, s, 42)
	var wg sync.WaitGroup
	errs := make(chan error, 32)
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); errs <- s.SetMembership(ctx, 42, v.ID, true) }()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	var count int
	if err := s.Pool.QueryRow(ctx, `SELECT count(*) FROM user_segments`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("count=%d err=%v", count, err)
	}
}
func TestConcurrentPatchPreservesFields(t *testing.T) {
	s := fixture(t, true)
	v := createSegment(t, s, "A")
	errs := make(chan error, 2)
	go func() {
		_, e := s.PatchSegment(context.Background(), v.ID, domain.PatchSegment{Name: str("New")})
		errs <- e
	}()
	go func() {
		_, e := s.PatchSegment(context.Background(), v.ID, domain.PatchSegment{Description: str("Changed")})
		errs <- e
	}()
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	got, err := s.GetSegment(context.Background(), v.ID)
	if err != nil || got.Name != "New" || got.Description != "Changed" {
		t.Fatalf("%+v %v", got, err)
	}
}
func TestCompositeIndexExists(t *testing.T) {
	s := fixture(t, true)
	var definition string
	err := s.Pool.QueryRow(context.Background(), `SELECT indexdef FROM pg_indexes WHERE schemaname=current_schema() AND indexname='user_segments_pkey'`).Scan(&definition)
	if err != nil || !strings.Contains(definition, "UNIQUE INDEX") || !strings.Contains(definition, "(user_id, segment_id)") {
		t.Fatalf("%s %v", definition, err)
	}
}
func TestDatabaseConstraints(t *testing.T) {
	s := fixture(t, true)
	for _, sql := range []string{`INSERT INTO users(id) VALUES(0)`, `INSERT INTO segments(code,name) VALUES('lowercase','Name')`, `INSERT INTO segments(code,name) VALUES('A',' ')`, `INSERT INTO user_segments(user_id,segment_id) VALUES(99,99)`} {
		if _, err := s.Pool.Exec(context.Background(), sql); err == nil {
			t.Fatalf("constraint missing for %s", sql)
		}
	}
}
func TestMigrationsIdempotentAndRollback(t *testing.T) {
	s := fixture(t, false)
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		if err := postgres.Migrate(ctx, s.Pool, "up"); err != nil {
			t.Fatal(err)
		}
	}
	createSegment(t, s, "A")
	if err := postgres.Migrate(ctx, s.Pool, "down"); err != nil {
		t.Fatal(err)
	}
	var exists bool
	if err := s.Pool.QueryRow(ctx, `SELECT to_regclass('distribution_jobs') IS NOT NULL`).Scan(&exists); err != nil || exists {
		t.Fatal("distribution rollback left table")
	}
	if err := postgres.Migrate(ctx, s.Pool, "down"); err != nil {
		t.Fatal(err)
	}
	if err := s.Pool.QueryRow(ctx, `SELECT to_regclass('segments') IS NOT NULL`).Scan(&exists); err != nil || exists {
		t.Fatal("rollback left table")
	}
	if err := postgres.Migrate(ctx, s.Pool, "up"); err != nil {
		t.Fatal(err)
	}
}
func TestMigrationsConcurrentStartup(t *testing.T) {
	s := fixture(t, false)
	errs := make(chan error, 4)
	for i := 0; i < 4; i++ {
		go func() { errs <- postgres.Migrate(context.Background(), s.Pool, "up") }()
	}
	for i := 0; i < 4; i++ {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	var n int
	if err := s.Pool.QueryRow(context.Background(), `SELECT count(*) FROM schema_migrations`).Scan(&n); err != nil || n != 2 {
		t.Fatalf("%d %v", n, err)
	}
}
func TestMigrationChecksumMismatch(t *testing.T) {
	s := fixture(t, true)
	if _, err := s.Pool.Exec(context.Background(), `UPDATE schema_migrations SET checksum='changed'`); err != nil {
		t.Fatal(err)
	}
	if err := postgres.Migrate(context.Background(), s.Pool, "up"); err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatal(err)
	}
}
func TestCanceledDatabaseRequest(t *testing.T) {
	s := fixture(t, true)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.GetSegment(ctx, 1); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
func request(t *testing.T, h http.Handler, method, path, body string, status int) []byte {
	t.Helper()
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	if body != "" {
		r.Header.Set("Content-Type", "application/json")
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != status {
		t.Fatalf("%s %s: expected %d, got %d: %s", method, path, status, w.Code, w.Body.String())
	}
	return w.Body.Bytes()
}
func TestHTTPFullScenario(t *testing.T) {
	s := fixture(t, true)
	h := httpapi.New(service.New(s), s.Pool.Ping, zap.NewNop(), time.Second)
	raw := request(t, h, "POST", "/segments", `{"code":"MAIL_GPT","name":"GPT","description":"Письма"}`, 201)
	var seg domain.Segment
	if err := json.Unmarshal(raw, &seg); err != nil {
		t.Fatal(err)
	}
	path := fmt.Sprintf("/segments/%d", seg.ID)
	request(t, h, "GET", path, "", 200)
	request(t, h, "GET", "/segments?limit=1", "", 200)
	request(t, h, "POST", "/users", `{"user_id":42}`, 201)
	request(t, h, "POST", "/users", `{"user_id":42}`, 409)
	request(t, h, "PUT", fmt.Sprintf("/users/42/segments/%d", seg.ID), "", 204)
	raw = request(t, h, "GET", "/users/42/segments", "", 200)
	if !strings.Contains(string(raw), "MAIL_GPT") {
		t.Fatal(string(raw))
	}
	request(t, h, "PATCH", path, `{"description":"Updated"}`, 200)
	request(t, h, "PATCH", path, `{"code":"NEW_CODE"}`, 400)
	request(t, h, "DELETE", fmt.Sprintf("/users/42/segments/%d", seg.ID), "", 204)
	request(t, h, "DELETE", path, "", 204)
	request(t, h, "GET", path, "", 404)
}
func TestHTTPValidation(t *testing.T) {
	s := fixture(t, true)
	h := httpapi.New(service.New(s), s.Pool.Ping, zap.NewNop(), time.Second)
	for _, tc := range []struct {
		name, method, path, body string
		status                   int
	}{{"unknown_field", "POST", "/segments", `{"code":"A","name":"x","extra":1}`, 400}, {"empty_name", "POST", "/segments", `{"code":"A","name":" "}`, 400}, {"null_segment", "POST", "/segments", `null`, 400}, {"zero_user", "POST", "/users", `{"user_id":0}`, 400}, {"null_user", "POST", "/users", `null`, 400}, {"overflow_user", "POST", "/users", `{"user_id":9223372036854775808}`, 400}, {"bad_path", "GET", "/segments/abc", "", 400}, {"zero_path", "GET", "/users/0/segments", "", 400}, {"bad_segment_path", "PUT", "/users/1/segments/x", "", 400}, {"bad_user_path", "DELETE", "/users/x/segments/1", "", 400}, {"empty_patch", "PATCH", "/segments/1", `{}`, 400}, {"null_patch", "PATCH", "/segments/1", `null`, 400}, {"null_field", "PATCH", "/segments/1", `{"name":null}`, 400}, {"bad_limit", "GET", "/segments?limit=abc", "", 400}, {"large_limit", "GET", "/segments?limit=1001", "", 400}, {"negative_cursor", "GET", "/segments?after_id=-1", "", 400}, {"bad_cursor", "GET", "/segments?after_id=x", "", 400}, {"missing_user", "GET", "/users/99/segments", "", 404}, {"empty_segments", "GET", "/segments", "", 200}} {
		t.Run(tc.name, func(t *testing.T) { request(t, h, tc.method, tc.path, tc.body, tc.status) })
	}
}

func TestConcurrentDeleteAndAssign(t *testing.T) {
	s := fixture(t, true)
	ctx := context.Background()
	segment := createSegment(t, s, "RACE")
	createUser(t, s, 42)
	start := make(chan struct{})
	assigned := make(chan error, 1)
	deleted := make(chan error, 1)
	go func() { <-start; assigned <- s.SetMembership(ctx, 42, segment.ID, true) }()
	go func() { <-start; deleted <- s.DeleteSegment(ctx, segment.ID) }()
	close(start)
	if err := <-deleted; err != nil {
		t.Fatal(err)
	}
	if err := <-assigned; err != nil && !errors.Is(err, domain.ErrNotFound) {
		t.Fatal(err)
	}
	got, err := s.UserSegments(ctx, 42)
	if err != nil || len(got) != 0 {
		t.Fatalf("unexpected remaining assignment: %+v %v", got, err)
	}
}
