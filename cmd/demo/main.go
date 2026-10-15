package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/Kasp3rR/segment-service-VK/internal/domain"
	"github.com/Kasp3rR/segment-service-VK/internal/postgres"
)

type Demo struct {
	BaseURL string
	Client  *http.Client
}

func main() {
	base := flag.String("url", "http://localhost:18080", "API URL")
	timeout := flag.Duration("timeout", 2*time.Minute, "deadline for the whole demonstration")
	flag.Parse()
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	demo := Demo{
		BaseURL: strings.TrimRight(*base, "/"),
		Client:  &http.Client{Timeout: 15 * time.Second},
	}
	if err := demo.Run(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func (d Demo) request(ctx context.Context, method string, path string, body string, target any) (int, error) {
	request, err := http.NewRequestWithContext(ctx, method, d.BaseURL+path, bytes.NewBufferString(body))
	if err != nil {
		return 0, err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := d.Client.Do(request)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		message, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return response.StatusCode, fmt.Errorf("%s %s: HTTP %d: %s", method, path, response.StatusCode, message)
	}
	if target != nil {
		err = json.NewDecoder(response.Body).Decode(target)
	}
	return response.StatusCode, err
}

func (d Demo) segment(ctx context.Context) (domain.Segment, error) {
	var segment domain.Segment
	status, err := d.request(ctx, http.MethodPost, "/segments", `{"code":"MAIL_GPT","name":"GPT в письмах","description":"Демонстрация 30%"}`, &segment)
	if err == nil || status != http.StatusConflict {
		return segment, err
	}
	var after int64
	for {
		var segments []domain.Segment
		_, err = d.request(ctx, http.MethodGet, fmt.Sprintf("/segments?limit=1000&after_id=%d", after), "", &segments)
		if err != nil {
			return segment, err
		}
		if len(segments) == 0 {
			return segment, fmt.Errorf("MAIL_GPT was not found after conflict")
		}
		for _, item := range segments {
			if item.Code == "MAIL_GPT" {
				return item, nil
			}
		}
		after = segments[len(segments)-1].ID
	}
}

func (d Demo) Run(ctx context.Context) error {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		return fmt.Errorf("DATABASE_URL is required for result verification")
	}
	store, err := postgres.Open(ctx, dsn, 1)
	if err != nil {
		return err
	}
	defer store.Pool.Close()
	segment, err := d.segment(ctx)
	if err != nil {
		return err
	}
	var job domain.Job
	_, err = d.request(ctx, http.MethodPost, fmt.Sprintf("/segments/%d/distributions", segment.ID), `{"percent":30}`, &job)
	if err != nil {
		return err
	}
	for job.Status != "completed" {
		if job.Status == "failed" {
			return fmt.Errorf("job %s failed: %s", job.ID, job.LastError)
		}
		timer := time.NewTimer(250 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		_, err = d.request(ctx, http.MethodGet, "/distributions/"+job.ID, "", &job)
		if err != nil {
			return err
		}
	}
	var storedJob domain.Job
	storedJob, err = store.GetDistribution(ctx, job.ID)
	if err != nil {
		return fmt.Errorf("verify API and DATABASE_URL refer to the same database: %w", err)
	}
	if storedJob.Status != "completed" {
		return fmt.Errorf("database job is not completed")
	}
	var actual int64
	var userID *int64
	err = store.Pool.QueryRow(ctx, `SELECT count(*),min(user_id) FROM user_segments WHERE segment_id=$1`, segment.ID).Scan(&actual, &userID)
	if err != nil {
		return err
	}
	expected := job.SnapshotUsers/100*30 + (job.SnapshotUsers%100)*30/100
	if actual != expected || job.TargetUsers != expected || job.ExistingUsers+job.AssignedUsers != expected {
		return fmt.Errorf("unexpected assignments: actual=%d target=%d expected=%d", actual, job.TargetUsers, expected)
	}
	if userID == nil {
		return fmt.Errorf("no assigned user to verify: the existing database must contain at least four users")
	}
	var segments []domain.Segment
	_, err = d.request(ctx, http.MethodGet, fmt.Sprintf("/users/%d/segments", *userID), "", &segments)
	if err != nil {
		return err
	}
	found := false
	for _, item := range segments {
		if item.ID == segment.ID {
			found = true
		}
	}
	if !found {
		return fmt.Errorf("user %d does not have MAIL_GPT in HTTP response", *userID)
	}
	return json.NewEncoder(os.Stdout).Encode(struct {
		Job      domain.Job       `json:"job"`
		Actual   int64            `json:"actual_assignments"`
		UserID   int64            `json:"verified_user_id"`
		Segments []domain.Segment `json:"user_segments"`
	}{job, actual, *userID, segments})
}
