package config

import (
	"os"
	"strconv"
)

type Config struct {
	Port          string
	WorkerCount   int
	QueueCapacity int
}

func Load() *Config {
	return &Config{
		Port:          getEnv("SERVER_PORT", "8080"),
		WorkerCount:   getEnvAsInt("WORKER_COUNT", 4), // runtime.NumCPU() will be our default later
		QueueCapacity: getEnvAsInt("QUEUE_CAPACITY", 10000),
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
