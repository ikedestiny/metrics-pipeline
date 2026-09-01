package config

import (
	"os"
	"strconv"
	"strings"
)

type Config struct {
	Port          string
	WorkerCount   int
	QueueCapacity int
	KafkaBrokers  []string
	KafkaTopic    string
}

func Load() *Config {
	brokersRaw := getEnv("KAFKA_BROKERS", "localhost:9092")
	return &Config{
		Port:          getEnv("SERVER_PORT", "8080"),
		WorkerCount:   getEnvAsInt("WORKER_COUNT", 4),
		QueueCapacity: getEnvAsInt("QUEUE_CAPACITY", 10000),
		KafkaBrokers:  strings.Split(brokersRaw, ","),
		KafkaTopic:    getEnv("KAFKA_TOPIC", "telemetry.metrics"),
	}
}

func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}

func getEnvAsInt(key string, defaultValue int) int {
	valueStr := getEnv(key, "")
	if value, err := strconv.Atoi(valueStr); err == nil {
		return value
	}
	return defaultValue
}
