package config

import (
	"log"
	"os"
	"strconv"
)

var (
	EnableBackup  = true
	EnableWriting = true
)

func init() {
	envEnableBackup := os.Getenv("EnableBackup")
	if envEnableBackup != "" {
		enableBackup, err := strconv.ParseBool(envEnableBackup)
		if err != nil {
			log.Fatalf("EnableBackup err parse bool: %s", err.Error())
		}

		EnableBackup = enableBackup
	}

	envEnableWriting := os.Getenv("EnableWriting")
	if envEnableWriting != "" {
		enableWriting, err := strconv.ParseBool(envEnableWriting)
		if err != nil {
			log.Fatalf("EnableWriting err parse bool: %s", err.Error())
		}

		EnableWriting = enableWriting
	}
}
