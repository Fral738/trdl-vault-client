package vault

import (
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/hashicorp/vault/api"
)

type TaskLogger func(taskID, msg string)

type TrdlClient struct {
	vaultClient *api.Client
}

type operationContext struct {
	trdlTaskStatus *TrdlTaskStatus
}

type TrdlTaskStatus struct {
	Status string `json:"status"`
	Reason string `json:"reason,omitempty"`
}

// NewTrdlClient initializes the Vault client using api.Config
func NewTrdlClient(config *api.Config, vaultToken string) (*TrdlClient, error) {
	client, err := api.NewClient(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create Vault client: %w", err)
	}

	client.SetToken(vaultToken)

	return &TrdlClient{vaultClient: client}, nil
}

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

func (c *TrdlClient) withBackoffRequest(
	path string,
	data map[string]interface{},
	taskLogger TaskLogger,
	action func(taskID string, taskLogger TaskLogger) error,
) error {
	config := c.vaultClient.CloneConfig()
	maxBackoff := config.MaxRetryWait
	backoff := config.MinRetryWait
	startTime := time.Now()

	for time.Since(startTime) < maxBackoff {
		resp, err := c.vaultClient.Logical().Write(path, data)
		if err == nil {
			taskID := resp.Data["task_uuid"].(string)
			return action(taskID, taskLogger)
		}

		if config.MaxRetries > 0 {
			log.Printf("[INFO] Retrying %s after %v...", path, backoff)
			time.Sleep(backoff)
			backoff = min(backoff*2, maxBackoff)
			continue
		}

		return fmt.Errorf("%s operation failed and retry is disabled: %w", path, err)
	}

	return fmt.Errorf("%s operation exceeded maximum duration", path)
}

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

func (c *TrdlClient) watchTask(projectName, taskID string, taskLogger TaskLogger) error {
	taskLogger(taskID, fmt.Sprintf("Started task %s", taskID))

	ctx := &operationContext{}

	errChan := make(chan error, 2)

	go func() {
		if err := c.watchTaskStatus(projectName, taskID); err != nil {
			errChan <- fmt.Errorf("error watching task %s status: %w", taskID, err)
		}
	}()

	go func() {
		if err := c.watchTaskLogs(projectName, taskID, taskLogger); err != nil {
			errChan <- fmt.Errorf("error watching task %s logs: %w", taskID, err)
		}
	}()

	for {
		select {
		case err := <-errChan:
			taskLogger(taskID, fmt.Sprintf("[ERROR] %s", err))
			return err
		default:
			if ctx.trdlTaskStatus != nil {
				switch ctx.trdlTaskStatus.Status {
				case "FAILED":
					return fmt.Errorf("task %s failed: %s", taskID, ctx.trdlTaskStatus.Reason)
				case "SUCCEEDED":
					return nil
				}
			}
			time.Sleep(200 * time.Millisecond)
		}
	}
}

func (c *TrdlClient) watchTaskStatus(projectName, taskID string) error {
	for {
		resp, err := c.vaultClient.Logical().Read(fmt.Sprintf("%s/task/%s", projectName, taskID))
		if err != nil {
			return fmt.Errorf("failed to fetch status for task %s: %w", taskID, err)
		}

		if resp == nil || resp.Data == nil {
			time.Sleep(500 * time.Millisecond)
			continue
		}

		status, ok := resp.Data["status"].(string)
		if !ok {
			time.Sleep(500 * time.Millisecond)
			continue
		}

		if status == "FAILED" {
			return fmt.Errorf("trdl task %s has failed: %v", taskID, resp.Data["reason"])
		}

		if status == "SUCCEEDED" {
			return nil
		}

		time.Sleep(500 * time.Millisecond)
	}
}

func (c *TrdlClient) watchTaskLogs(projectName, taskID string, taskLogger TaskLogger) error {
	cursor := 0
	for {
		data := map[string][]string{
			"limit":  {"1000000000"},
			"offset": {fmt.Sprintf("%d", cursor)},
		}

		resp, err := c.vaultClient.Logical().ReadWithData(
			fmt.Sprintf("%s/task/%s/log", projectName, taskID),
			data,
		)
		if err != nil {
			return fmt.Errorf("failed to fetch logs for task %s: %w", taskID, err)
		}

		if resp == nil || resp.Data == nil {
			time.Sleep(500 * time.Millisecond)
			continue
		}

		logs, ok := resp.Data["result"].(string)
		if !ok {
			time.Sleep(500 * time.Millisecond)
			continue
		}

		if len(logs) == 0 {
			time.Sleep(500 * time.Millisecond)
			continue
		}

		logLines := strings.Split(strings.TrimSpace(logs), "\n")
		for _, line := range logLines {
			taskLogger(taskID, line)
		}

		cursor += len(logs)
		time.Sleep(500 * time.Millisecond)
	}
}
