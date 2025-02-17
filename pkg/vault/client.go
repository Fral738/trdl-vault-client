package vault

import (
	"fmt"
	"os"

	"github.com/cenkalti/backoff/v4"
	"github.com/hashicorp/vault/api"
)

type TaskLogger func(taskID, msg string)

type TrdlClient struct {
	vaultClient *api.Client
}

// NewTrdlClient initializes the Vault client using DefaultConfig
func NewTrdlClient(vaultToken string) (*TrdlClient, error) {
	config := api.DefaultConfig()

	if addr := os.Getenv("VAULT_ADDR"); addr != "" {
		config.Address = addr
	}

	client, err := api.NewClient(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create Vault client: %w", err)
	}

	client.SetToken(vaultToken)

	return &TrdlClient{vaultClient: client}, nil
}

// Publish sends a publish request to Vault
func (c *TrdlClient) Publish(projectName string, taskLogger TaskLogger) error {
	path := fmt.Sprintf("%s/publish", projectName)

	resp, err := c.vaultClient.Logical().Write(path, nil)
	if err != nil {
		return fmt.Errorf("failed to start publish task: %w", err)
	}

	taskID, ok := resp.Data["task_uuid"].(string)
	if !ok {
		return fmt.Errorf("invalid response from Vault: missing task_uuid")
	}

	return c.watchTask(projectName, taskID, taskLogger)
}

// Release sends a release request to Vault
func (c *TrdlClient) Release(projectName, gitTag string, taskLogger TaskLogger) error {
	path := fmt.Sprintf("%s/release", projectName)
	data := map[string]interface{}{"git_tag": gitTag}

	resp, err := c.vaultClient.Logical().Write(path, data)
	if err != nil {
		return fmt.Errorf("failed to start release task: %w", err)
	}

	taskID, ok := resp.Data["task_uuid"].(string)
	if !ok {
		return fmt.Errorf("invalid response from Vault: missing task_uuid")
	}

	return c.watchTask(projectName, taskID, taskLogger)
}

// watchTask waits for the task to finish and handles status changes
func (c *TrdlClient) watchTask(projectName, taskID string, taskLogger TaskLogger) error {
	taskLogger(taskID, fmt.Sprintf("Started task %s", taskID))

	// Backoff strategy for retrying failed operations
	operation := func() error {
		status, reason, err := c.getTaskStatus(projectName, taskID)
		if err != nil {
			return err
		}

		switch status {
		case "RUNNING":
			taskLogger(taskID, fmt.Sprintf("Task %s is still running...", taskID))
		case "FAILED":
			// Log the reason for failure
			taskLogger(taskID, fmt.Sprintf("Task %s failed: %s", taskID, reason))

			// Retry on certain errors (e.g., signature verification failure)
			if reason == "signature verification failed" {
				taskLogger(taskID, "Retrying due to signature verification failure...")
				return fmt.Errorf("retrying...")
			}
			_ = c.getTaskLogs(projectName, taskID, taskLogger)
			return fmt.Errorf("task %s failed: %s", taskID, reason)
		case "SUCCEEDED":
			_ = c.getTaskLogs(projectName, taskID, taskLogger)
			return nil
		}
		return nil
	}

	// Use exponential backoff for retrying the operation
	err := backoff.Retry(operation, backoff.NewExponentialBackOff())
	if err != nil {
		return fmt.Errorf("task %s failed after retries: %w", taskID, err)
	}

	return nil
}

// getTaskStatus retrieves the status of the task
func (c *TrdlClient) getTaskStatus(projectName, taskID string) (string, string, error) {
	resp, err := c.vaultClient.Logical().Read(fmt.Sprintf("%s/task/%s", projectName, taskID))
	if err != nil {
		return "", "", fmt.Errorf("failed to fetch task status: %w", err)
	}
	if resp == nil || resp.Data == nil {
		return "", "", nil
	}

	status, _ := resp.Data["status"].(string)
	reason, _ := resp.Data["reason"].(string)

	return status, reason, nil
}

// getTaskLogs retrieves the logs of the task
func (c *TrdlClient) getTaskLogs(projectName, taskID string, taskLogger TaskLogger) error {
	resp, err := c.vaultClient.Logical().Read(fmt.Sprintf("%s/task/%s/log", projectName, taskID))
	if err != nil {
		return fmt.Errorf("failed to fetch task logs: %w", err)
	}
	if resp == nil || resp.Data == nil {
		return nil
	}

	logs, ok := resp.Data["result"].(string)
	if !ok || logs == "" {
		return nil
	}
	taskLogger(taskID, logs)
	return nil
}
