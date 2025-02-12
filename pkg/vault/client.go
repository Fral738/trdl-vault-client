package vault

import (
	"fmt"
	"log"
	"strings"
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

type operationContext struct {
	contextCancelled      bool
	watchTaskLogsActive   bool
	watchTaskStatusActive bool
	trdlTaskStatus        *TrdlTaskStatus
	err                   error
}

type TrdlTaskStatus struct {
	Status string `json:"status"`
	Reason string `json:"reason,omitempty"`
}

func newOperationContext() *operationContext {
	return &operationContext{
		contextCancelled:      false,
		watchTaskLogsActive:   false,
		watchTaskStatusActive: false,
		trdlTaskStatus:        nil,
	}
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

func (c *TrdlClient) gracefulShutdown(ctx *operationContext) {
	ctx.contextCancelled = true

	for {
		if !ctx.watchTaskStatusActive && !ctx.watchTaskLogsActive {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// watchTask monitors the status and logs of a task
func (c *TrdlClient) watchTask(projectName, taskID string, taskLogger TaskLogger) error {
	taskLogger(taskID, fmt.Sprintf("Started task %s", taskID))

	ctx := newOperationContext()

	errChan := make(chan error, 2)

	go func() {
		if err := c.watchTaskStatus(ctx, projectName, taskID); err != nil {
			errChan <- fmt.Errorf("error watching task %s status: %w", taskID, err)
		}
	}()

	go func() {
		if err := c.watchTaskLogs(ctx, projectName, taskID, taskLogger); err != nil {
			errChan <- fmt.Errorf("error watching task %s logs: %w", taskID, err)
		}
	}()

	// Ожидание завершения таски или ошибки
	for {
		select {
		case err := <-errChan:
			// Ошибка в одной из горутин
			c.gracefulShutdown(ctx)
			taskLogger(taskID, fmt.Sprintf("[ERROR] %s", err))
			return err

		default:
			if ctx.trdlTaskStatus != nil {
				switch ctx.trdlTaskStatus.Status {
				case "FAILED":
					c.gracefulShutdown(ctx)
					return fmt.Errorf("task %s failed: %s", taskID, ctx.trdlTaskStatus.Reason)

				case "SUCCEEDED":
					c.gracefulShutdown(ctx)
					return nil
				}
			}
			time.Sleep(200 * time.Millisecond)
		}
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

func (c *TrdlClient) Release(projectName, gitTag string, taskLogger TaskLogger) error {
	return c.withBackoffRequest(
		fmt.Sprintf("v1/%s/release", projectName),
		map[string]interface{}{"git_tag": gitTag},
		taskLogger,
		func(taskID string, taskLogger TaskLogger) error {
			return c.watchTask(projectName, taskID, taskLogger)
		},
	)
}

// watchTaskStatus periodically fetches task status and updates context
func (c *TrdlClient) watchTaskStatus(ctx *operationContext, projectName, taskID string) error {
	ctx.watchTaskStatusActive = true
	defer func() { ctx.watchTaskStatusActive = false }()

	for {
		if ctx.contextCancelled {
			break
		}

		resp, err := c.vaultClient.Logical().Read(fmt.Sprintf("v1/%s/task/%s", projectName, taskID))
		if err != nil {
			return fmt.Errorf("failed to get task status: %w", err)
		}

		if resp == nil || resp.Data == nil {
			return fmt.Errorf("task %s status response is empty", taskID)
		}

		status, ok := resp.Data["status"].(string)
		if !ok {
			return fmt.Errorf("unexpected response format for task %s status", taskID)
		}

		ctx.trdlTaskStatus = &TrdlTaskStatus{
			Status: status,
			Reason: resp.Data["reason"].(string),
		}

		time.Sleep(500 * time.Millisecond)
	}
	return nil
}

// watchTaskLogs fetches logs in a loop
func (c *TrdlClient) watchTaskLogs(ctx *operationContext, projectName, taskID string, taskLogger TaskLogger) error {
	ctx.watchTaskLogsActive = true
	defer func() { ctx.watchTaskLogsActive = false }()

	cursor := 0
	for {
		if ctx.contextCancelled {
			break
		}

		resp, err := c.vaultClient.Logical().ReadWithData(
			fmt.Sprintf("v1/%s/task/%s/log", projectName, taskID),
			map[string][]string{"qs": {"limit=1000000000", fmt.Sprintf("offset=%d", cursor)}},
		)
		if err != nil {
			return fmt.Errorf("failed to fetch logs for task %s: %w", taskID, err)
		}

		if resp == nil || resp.Data == nil {
			return fmt.Errorf("empty logs response for task %s", taskID)
		}

		logs, ok := resp.Data["result"].(string)
		if ok && len(logs) > 0 {
			logLines := strings.Split(strings.TrimSpace(logs), "\n")
			for _, line := range logLines {
				taskLogger(taskID, line)
			}
			cursor += len(logs)
		}

		time.Sleep(500 * time.Millisecond)
	}
	return nil
}
