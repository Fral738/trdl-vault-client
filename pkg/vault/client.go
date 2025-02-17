package vault

import (
	"fmt"
	"os"
	"time"

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

func (c *TrdlClient) withBackoffRequest(
	path string,
	data map[string]interface{},
	taskLogger TaskLogger,
	action func(taskID string, taskLogger TaskLogger) error,
) error {
	bo := backoff.NewExponentialBackOff()
	bo.InitialInterval = 1 * time.Minute
	bo.MaxInterval = 10 * time.Minute
	bo.MaxElapsedTime = 30 * time.Minute
	bo.Multiplier = 2

	operation := func() error {
		resp, err := c.vaultClient.Logical().Write(path, data)
		if err != nil {
			taskLogger("", fmt.Sprintf("[ERROR] %v", err))
			return err
		}

		taskID, ok := resp.Data["task_uuid"].(string)
		if !ok {
			return fmt.Errorf("invalid response from Vault: missing task_uuid")
		}

		return action(taskID, taskLogger)
	}

	if err := backoff.Retry(operation, bo); err != nil {
		return fmt.Errorf("%s operation exceeded maximum duration: %w", path, err)
	}

	return nil
}

// Publish sends a publish request to Vault
func (c *TrdlClient) Publish(projectName string, taskLogger TaskLogger) error {
	return c.withBackoffRequest(
		fmt.Sprintf("%s/publish", projectName),
		nil,
		taskLogger,
		func(taskID string, taskLogger TaskLogger) error {
			return c.watchTask(projectName, taskID, taskLogger)
		},
	)
}

// Release sends a release request to Vault
func (c *TrdlClient) Release(projectName, gitTag string, taskLogger TaskLogger) error {
	return c.withBackoffRequest(
		fmt.Sprintf("%s/release", projectName),
		map[string]interface{}{"git_tag": gitTag},
		taskLogger,
		func(taskID string, taskLogger TaskLogger) error {
			return c.watchTask(projectName, taskID, taskLogger)
		},
	)
}

// watchTask waits for the task to finish and handles status changes
func (c *TrdlClient) watchTask(projectName, taskID string, taskLogger TaskLogger) error {
	taskLogger(taskID, fmt.Sprintf("Started task %s", taskID))

	for {
		status, reason, err := c.getTaskStatus(projectName, taskID)
		if err != nil {
			return fmt.Errorf("failed to fetch task status for %s: %w", taskID, err)
		}

		taskLogger(taskID, fmt.Sprintf("Current status of task %s: %s", taskID, status))

		switch status {
		case "FAILED":
			taskLogger(taskID, fmt.Sprintf("Task %s failed: %s", taskID, reason))
			_ = c.getTaskLogs(projectName, taskID, taskLogger)
			return fmt.Errorf("task %s failed: %s", taskID, reason)
		case "SUCCEEDED":
			_ = c.getTaskLogs(projectName, taskID, taskLogger)
			return nil
		default:
			time.Sleep(2 * time.Second)
		}
	}
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
