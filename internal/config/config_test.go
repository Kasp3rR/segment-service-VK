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

func TestKafkaDefaults(t *testing.T) {
	c, err := Parse(func(k string) string {
		if k == "DATABASE_URL" {
			return "postgres://db"
		}
		return ""
	})
	if err != nil || len(c.KafkaBrokers) != 1 || c.KafkaBrokers[0] != "localhost:29092" || c.KafkaTopic != "segment-distributions" || c.KafkaGroup != "segment-service" || c.WorkerHTTPAddr != ":8082" || c.OutboxPollInterval != 250*time.Millisecond || c.WorkerTimeout != 15*time.Second {
		t.Fatalf("%+v %v", c, err)
	}
}
func TestKafkaOverrides(t *testing.T) {
	env := map[string]string{"DATABASE_URL": "postgres://db", "KAFKA_BROKERS": "one:9092, two:9093", "KAFKA_TOPIC": "batches", "KAFKA_GROUP": "workers", "WORKER_HTTP_ADDR": ":18082", "OUTBOX_POLL_INTERVAL": "50ms", "WORKER_TIMEOUT": "20s"}
	c, err := Parse(func(k string) string { return env[k] })
	if err != nil || len(c.KafkaBrokers) != 2 || c.KafkaBrokers[1] != "two:9093" || c.KafkaTopic != "batches" || c.KafkaGroup != "workers" || c.WorkerHTTPAddr != ":18082" || c.OutboxPollInterval != 50*time.Millisecond || c.WorkerTimeout != 20*time.Second {
		t.Fatalf("%+v %v", c, err)
	}
}
func TestInvalidKafkaConfiguration(t *testing.T) {
	for _, tc := range []struct{ key, value string }{{"KAFKA_BROKERS", "host"}, {"KAFKA_BROKERS", ":9092"}, {"KAFKA_BROKERS", "host:0"}, {"KAFKA_BROKERS", "host:65536"}, {"KAFKA_BROKERS", "host:x"}, {"WORKER_HTTP_ADDR", "8082"}, {"WORKER_HTTP_ADDR", ":0"}, {"OUTBOX_POLL_INTERVAL", "0s"}, {"OUTBOX_POLL_INTERVAL", "bad"}, {"WORKER_TIMEOUT", "-1s"}, {"WORKER_TIMEOUT", "5s"}} {
		t.Run(tc.key+tc.value, func(t *testing.T) {
			env := map[string]string{"DATABASE_URL": "postgres://db", tc.key: tc.value}
			if _, err := Parse(func(k string) string { return env[k] }); err == nil {
				t.Fatal("invalid configuration accepted")
			}
		})
	}
}
