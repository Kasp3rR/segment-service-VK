package main

import "testing"

func TestBenchmarkRejectsInvalidSize(t *testing.T) {
	for _, test := range []struct {
		users   int64
		repeats int
	}{{0, 20}, {9, 20}, {10000001, 20}, {10, 0}, {10, 1001}} {
		if err := run(test.users, test.repeats, t.TempDir()); err == nil {
			t.Fatalf("accepted %+v", test)
		}
	}
}

func TestBenchmarkRequiresTestDatabase(t *testing.T) {
	t.Setenv("TEST_DATABASE_URL", "")
	if err := run(10, 1, t.TempDir()); err == nil {
		t.Fatal("missing database accepted")
	}
}
