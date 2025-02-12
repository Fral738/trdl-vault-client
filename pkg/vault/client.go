package vault

import (
	"fmt"
	"log"
	"time"

	"github.com/hashicorp/vault/api"
)

type TaskLogger func(taskID, msg string)

type TrdlClientOptions struct {
	VaultAddr   string
	VaultToken  string
	Retry       bool
	MaxDelaySec int
}

type TrdlClient struct {
	vaultClient *api.Client
	vaultToken  string
	retry       bool
	maxDelay    time.Duration
}

func NewTrdlClient(opts TrdlClientOptions) (*TrdlClient, error) {
	config := &api.Config{Address: opts.VaultAddr}
	client, err := api.NewClient(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create Vault client: %w", err)
	}

	client.SetToken(opts.VaultToken)

	return &TrdlClient{
		vaultClient: client,
		vaultToken:  opts.VaultToken,
		retry:       opts.Retry,
		maxDelay:    time.Duration(opts.MaxDelaySec) * time.Second,
	}, nil
}

func (c *TrdlClient) Publish(projectName string, taskLogger TaskLogger) error {
	return c.withBackoffRequest(
		fmt.Sprintf("v1/%s/publish", projectName),
		nil,
		taskLogger,
		func(taskID string, taskLogger TaskLogger) error {
			return c.watchTask(projectName, taskID, taskLogger)
		},
	)
}

func (c *TrdlClient) withBackoffRequest(
	path string,
	data map[string]interface{},
	taskLogger TaskLogger,
	action func(taskID string, taskLogger TaskLogger) error,
) error {
	maxBackoff := c.maxDelay
	backoff := 60 * time.Second
	startTime := time.Now()

	for time.Since(startTime) < maxBackoff {
		resp, err := c.vaultClient.Logical().Write(path, data)
		if err == nil {
			taskID := resp.Data["task_uuid"].(string)
			return action(taskID, taskLogger)
		}

		if c.retry {
			log.Printf("[INFO] Retrying %s after %v...", path, backoff)
			time.Sleep(backoff)
			backoff = min(backoff*2, maxBackoff)
			continue
		}

		return fmt.Errorf("%s operation failed and retry is disabled: %w", path, err)
	}

	return fmt.Errorf("%s operation exceeded maximum duration", path)
}

func (c *TrdlClient) watchTask(projectName, taskID string, taskLogger TaskLogger) error {
	taskLogger(taskID, fmt.Sprintf("Started publish task %s", taskID))

	for {
		status, reason, err := c.getTaskStatus(projectName, taskID)
		if err != nil {
			return fmt.Errorf("error getting task %s status: %w", taskID, err)
		}

		if status == "FAILED" {
			logs, _ := c.getTaskLogs(projectName, taskID)
			taskLogger(taskID, logs)
			return fmt.Errorf("task %s failed: %s", taskID, reason)
		}

		if status == "SUCCEEDED" {
			logs, _ := c.getTaskLogs(projectName, taskID)
			taskLogger(taskID, logs)
			return nil
		}

		time.Sleep(200 * time.Millisecond)
	}
}

func (c *TrdlClient) getTaskStatus(projectName, taskID string) (string, string, error) {
	path := fmt.Sprintf("v1/%s/task/%s", projectName, taskID)
	resp, err := c.vaultClient.Logical().Read(path)
	if err != nil {
		return "", "", err
	}

	if resp == nil || resp.Data == nil {
		return "", "", fmt.Errorf("unexpected empty response")
	}

	status := resp.Data["status"].(string)
	reason, _ := resp.Data["reason"].(string)

	return status, reason, nil
}

func (c *TrdlClient) getTaskLogs(projectName, taskID string) (string, error) {
	path := fmt.Sprintf("v1/%s/task/%s/log?limit=0", projectName, taskID)
	resp, err := c.vaultClient.Logical().Read(path)
	if err != nil {
		return "", err
	}

	if resp == nil || resp.Data == nil {
		return "", fmt.Errorf("unexpected empty response")
	}

	logs, _ := resp.Data["result"].(string)
	return logs, nil
}
