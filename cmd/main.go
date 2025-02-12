package main

import (
	"log"
	"os"

	"trdl-vault-client/pkg/vault"
)

func main() {
	vaultAddr := os.Getenv("VAULT_ADDR")
	vaultToken := os.Getenv("VAULT_TOKEN")
	projectName := os.Getenv("TRDL_RELEASE_PROJECT_NAME")

	client, err := vault.NewTrdlClient(vault.TrdlClientOptions{
		VaultAddr:   vaultAddr,
		VaultToken:  vaultToken,
		Retry:       true,
		MaxDelaySec: 300,
	})
	if err != nil {
		log.Fatalf("Error: %v", err)
	}

	log.Println("Before publish")

	err = client.Publish(projectName, func(taskID, msg string) {
		log.Printf("[%s] %s", taskID, msg)
	})
	if err != nil {
		log.Fatalf("Publish failed: %v", err)
	}

	log.Println("Publish done!")
}
