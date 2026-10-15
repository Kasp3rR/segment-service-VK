package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDemoCreatesSegment(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/segments" {
			t.Error("unexpected request")
		}
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte(`{"id":7,"code":"MAIL_GPT"}`))
	}))
	defer server.Close()
	demo := Demo{BaseURL: server.URL, Client: server.Client()}
	segment, err := demo.segment(context.Background())
	if err != nil || segment.ID != 7 {
		t.Fatalf("%+v %v", segment, err)
	}
}

func TestDemoFindsExistingSegmentAcrossPages(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			w.WriteHeader(http.StatusConflict)
			return
		}
		switch r.URL.Query().Get("after_id") {
		case "0":
			w.Write([]byte(`[{"id":3,"code":"OTHER"}]`))
		case "3":
			w.Write([]byte(`[{"id":8,"code":"MAIL_GPT"}]`))
		default:
			t.Error("incorrect pagination")
			w.Write([]byte(`[]`))
		}
	}))
	defer server.Close()
	demo := Demo{BaseURL: server.URL, Client: server.Client()}
	segment, err := demo.segment(context.Background())
	if err != nil || segment.ID != 8 {
		t.Fatalf("%+v %v", segment, err)
	}
}

func TestDemoReportsHTTPFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, "active job", http.StatusConflict) }))
	defer server.Close()
	demo := Demo{BaseURL: server.URL, Client: server.Client()}
	status, err := demo.request(context.Background(), http.MethodPost, "/segments/1/distributions", `{"percent":30}`, nil)
	if status != http.StatusConflict || err == nil || !strings.Contains(err.Error(), "active job") {
		t.Fatalf("%d %v", status, err)
	}
}

func TestDemoRejectsBrokenResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(`{broken`)) }))
	defer server.Close()
	demo := Demo{BaseURL: server.URL, Client: server.Client()}
	if _, err := demo.segment(context.Background()); err == nil {
		t.Fatal("invalid response accepted")
	}
}

func TestDemoHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	demo := Demo{BaseURL: "http://127.0.0.1:1", Client: http.DefaultClient}
	if _, err := demo.request(ctx, http.MethodGet, "/segments", "", nil); err == nil {
		t.Fatal("cancelled request succeeded")
	}
}

func TestDemoRequiresVerificationDatabase(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	if err := (Demo{}).Run(context.Background()); err == nil {
		t.Fatal("missing database accepted")
	}
}
