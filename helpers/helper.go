package helpers

import "os"

func GetEnv(key string) string {
	val, ok := os.LookupEnv(key)
	if !ok {
		val = os.Getenv(key)
	}
	return val
}
