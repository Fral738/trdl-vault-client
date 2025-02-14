package main

import (
	"log"
	"net/http"
	"os"
	"time"

	"trdl-vault-client/pkg/vault"

	"github.com/hashicorp/vault/api"
)

func main() {
	vaultAddr := os.Getenv("VAULT_ADDR")
	vaultToken := os.Getenv("VAULT_TOKEN")
	projectName := os.Getenv("TRDL_RELEASE_PROJECT_NAME")
	gitTag := os.Getenv("TRDL_GIT_TAG")

	config := &api.Config{
		Address:    vaultAddr,
		Timeout:    60 * time.Second,
		HttpClient: &http.Client{Timeout: 60 * time.Second},
	}

	client, err := vault.NewTrdlClient(config, vaultToken)
	if err != nil {
		log.Fatalf("Error: %v", err)
	}

	taskLogger := func(taskID, msg string) {
		log.Printf("[%s] %s", taskID, msg)
	}

	switch os.Getenv("TRDL_OPERATION") {
	case "publish":
		log.Println("Starting publish...")
		err = client.Publish(projectName, taskLogger)
	case "release":
		log.Println("Starting release...")
		err = client.Release(projectName, gitTag, taskLogger)
	default:
		log.Fatalf("Unknown operation. Set TRDL_OPERATION to 'publish' or 'release'.")
	}

	if err != nil {
		log.Fatalf("Operation failed: %v", err)
	}

	log.Println("Operation completed successfully!")
}
