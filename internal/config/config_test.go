package config

import (
	"testing"
	"time"
)

func TestDefaults(t *testing.T) {
	c, e := Parse(func(k string) string {
		if k == "DATABASE_URL" {
			return "postgres://local/db"
		}
		return ""
	})
	if e != nil || c.HTTPAddr != ":8080" || c.MaxConnections != 10 || c.RequestTimeout != 10*time.Second {
		t.Fatalf("%+v %v", c, e)
	}
}
func TestOverrides(t *testing.T) {
	env := map[string]string{"DATABASE_URL": "postgres://db", "HTTP_ADDR": "127.0.0.1:18080", "DB_MAX_CONNS": "4", "REQUEST_TIMEOUT": "3s", "SHUTDOWN_TIMEOUT": "2s"}
	c, e := Parse(func(k string) string { return env[k] })
	if e != nil || c.MaxConnections != 4 || c.RequestTimeout != 3*time.Second || c.ShutdownTimeout != 2*time.Second {
		t.Fatalf("%+v %v", c, e)
	}
}
func TestInvalidConfiguration(t *testing.T) {
	for _, tc := range []struct{ key, value string }{{"DATABASE_URL", ""}, {"HTTP_ADDR", "8080"}, {"HTTP_ADDR", ":0"}, {"HTTP_ADDR", ":65536"}, {"DB_MAX_CONNS", "0"}, {"DB_MAX_CONNS", "101"}, {"DB_MAX_CONNS", "no"}, {"REQUEST_TIMEOUT", "0s"}, {"REQUEST_TIMEOUT", "bad"}, {"SHUTDOWN_TIMEOUT", "-1s"}} {
		t.Run(tc.key+tc.value, func(t *testing.T) {
			env := map[string]string{"DATABASE_URL": "postgres://db", tc.key: tc.value}
			if _, e := Parse(func(k string) string { return env[k] }); e == nil {
				t.Fatal("accepted invalid configuration")
			}
		})
	}
}
