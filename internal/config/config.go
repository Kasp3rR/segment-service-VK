package config

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"time"
)

type Config struct {
	DatabaseURL, HTTPAddr           string
	MaxConnections                  int32
	RequestTimeout, ShutdownTimeout time.Duration
}

func Load() (Config, error) { return Parse(os.Getenv) }
func Parse(get func(string) string) (Config, error) {
	c := Config{DatabaseURL: get("DATABASE_URL"), HTTPAddr: get("HTTP_ADDR"), MaxConnections: 10, RequestTimeout: 10 * time.Second, ShutdownTimeout: 10 * time.Second}
	if c.DatabaseURL == "" {
		return c, fmt.Errorf("DATABASE_URL is required")
	}
	if c.HTTPAddr == "" {
		c.HTTPAddr = ":8080"
	}
	if _, port, e := net.SplitHostPort(c.HTTPAddr); e != nil {
		return c, fmt.Errorf("HTTP_ADDR must be host:port")
	} else if n, e := strconv.Atoi(port); e != nil || n < 1 || n > 65535 {
		return c, fmt.Errorf("HTTP_ADDR port must be 1..65535")
	}
	if v := get("DB_MAX_CONNS"); v != "" {
		n, e := strconv.ParseInt(v, 10, 32)
		if e != nil || n < 1 || n > 100 {
			return c, fmt.Errorf("DB_MAX_CONNS must be 1..100")
		}
		c.MaxConnections = int32(n)
	}
	for _, x := range []struct {
		name string
		dst  *time.Duration
	}{{"REQUEST_TIMEOUT", &c.RequestTimeout}, {"SHUTDOWN_TIMEOUT", &c.ShutdownTimeout}} {
		if v := get(x.name); v != "" {
			d, e := time.ParseDuration(v)
			if e != nil || d <= 0 {
				return c, fmt.Errorf("%s must be a positive duration", x.name)
			}
			*x.dst = d
		}
	}
	return c, nil
}
