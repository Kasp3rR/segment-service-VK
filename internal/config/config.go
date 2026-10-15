package config

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	DatabaseURL        string
	HTTPAddr           string
	MaxConnections     int32
	RequestTimeout     time.Duration
	ShutdownTimeout    time.Duration
	KafkaBrokers       []string
	KafkaTopic         string
	KafkaGroup         string
	WorkerHTTPAddr     string
	OutboxPollInterval time.Duration
	WorkerTimeout      time.Duration
}

func Load() (Config, error) {
	return Parse(os.Getenv)
}

func Parse(get func(string) string) (Config, error) {
	config := Config{
		DatabaseURL:    get("DATABASE_URL"),
		HTTPAddr:       valueOrDefault(get("HTTP_ADDR"), ":8080"),
		MaxConnections: 10,
		KafkaTopic:     valueOrDefault(get("KAFKA_TOPIC"), "segment-distributions"),
		KafkaGroup:     valueOrDefault(get("KAFKA_GROUP"), "segment-service"),
		WorkerHTTPAddr: valueOrDefault(get("WORKER_HTTP_ADDR"), ":8082"),
	}
	if config.DatabaseURL == "" {
		return config, fmt.Errorf("DATABASE_URL is required")
	}
	err := validateAddress(config.HTTPAddr, "HTTP_ADDR", false)
	if err != nil {
		return config, err
	}
	err = validateAddress(config.WorkerHTTPAddr, "WORKER_HTTP_ADDR", false)
	if err != nil {
		return config, err
	}
	if value := get("DB_MAX_CONNS"); value != "" {
		connections, err := strconv.ParseInt(value, 10, 32)
		if err != nil || connections < 1 || connections > 100 {
			return config, fmt.Errorf("DB_MAX_CONNS must be 1..100")
		}
		config.MaxConnections = int32(connections)
	}
	config.RequestTimeout, err = parseDuration(get("REQUEST_TIMEOUT"), "REQUEST_TIMEOUT", 10*time.Second)
	if err != nil {
		return config, err
	}
	config.ShutdownTimeout, err = parseDuration(get("SHUTDOWN_TIMEOUT"), "SHUTDOWN_TIMEOUT", 10*time.Second)
	if err != nil {
		return config, err
	}
	config.OutboxPollInterval, err = parseDuration(get("OUTBOX_POLL_INTERVAL"), "OUTBOX_POLL_INTERVAL", 250*time.Millisecond)
	if err != nil {
		return config, err
	}
	config.WorkerTimeout, err = parseDuration(get("WORKER_TIMEOUT"), "WORKER_TIMEOUT", 15*time.Second)
	if err != nil {
		return config, err
	}
	if config.WorkerTimeout < 6*time.Second {
		return config, fmt.Errorf("WORKER_TIMEOUT must be at least 6s")
	}
	brokers := valueOrDefault(get("KAFKA_BROKERS"), "localhost:29092")
	for _, address := range strings.Split(brokers, ",") {
		address = strings.TrimSpace(address)
		err = validateAddress(address, "KAFKA_BROKERS", true)
		if err != nil {
			return config, err
		}
		config.KafkaBrokers = append(config.KafkaBrokers, address)
	}
	return config, nil
}

func valueOrDefault(value string, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func parseDuration(value string, name string, fallback time.Duration) (time.Duration, error) {
	if value == "" {
		return fallback, nil
	}
	duration, err := time.ParseDuration(value)
	if err != nil || duration <= 0 {
		return 0, fmt.Errorf("%s must be a positive duration", name)
	}
	return duration, nil
}

func validateAddress(address string, name string, requireHost bool) error {
	host, port, err := net.SplitHostPort(address)
	if err != nil || (requireHost && host == "") {
		return fmt.Errorf("%s must be host:port", name)
	}
	number, err := strconv.Atoi(port)
	if err != nil || number < 1 || number > 65535 {
		return fmt.Errorf("%s port must be 1..65535", name)
	}
	return nil
}
