// Package emr wraps the EMR on EKS (emr-containers) API.
package emr

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/emrcontainers"
	"github.com/aws/aws-sdk-go-v2/service/emrcontainers/types"
)

// Result of a cancel request.
type Result string

const (
	Cancelled       Result = "cancelled"        // CancelJobRun was called
	AlreadyTerminal Result = "already-terminal" // job run had already finished or is being cancelled
)

// JobCanceller cancels EMR job runs.
type JobCanceller interface {
	Cancel(ctx context.Context, virtualClusterID, jobID string) (Result, error)
}

type api interface {
	DescribeJobRun(ctx context.Context, in *emrcontainers.DescribeJobRunInput, opts ...func(*emrcontainers.Options)) (*emrcontainers.DescribeJobRunOutput, error)
	CancelJobRun(ctx context.Context, in *emrcontainers.CancelJobRunInput, opts ...func(*emrcontainers.Options)) (*emrcontainers.CancelJobRunOutput, error)
}

// Client implements JobCanceller with the AWS SDK.
type Client struct{ api api }

// New builds a Client from the default credential chain (IRSA / Pod Identity on EKS).
func New(ctx context.Context) (*Client, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("loading AWS config: %w", err)
	}
	if cfg.Region == "" {
		return nil, errors.New("AWS region not set (AWS_REGION)")
	}
	return &Client{api: emrcontainers.NewFromConfig(cfg)}, nil
}

// Cancel checks the job run state first so we never cancel a run that has
// already finished (or is already being cancelled).
func (c *Client) Cancel(ctx context.Context, virtualClusterID, jobID string) (Result, error) {
	out, err := c.api.DescribeJobRun(ctx, &emrcontainers.DescribeJobRunInput{
		Id:               aws.String(jobID),
		VirtualClusterId: aws.String(virtualClusterID),
	})
	if err != nil {
		return "", fmt.Errorf("describe-job-run %s/%s: %w", virtualClusterID, jobID, err)
	}
	if out.JobRun != nil {
		switch out.JobRun.State {
		case types.JobRunStatePending, types.JobRunStateSubmitted, types.JobRunStateRunning:
		default:
			return AlreadyTerminal, nil
		}
	}
	if _, err := c.api.CancelJobRun(ctx, &emrcontainers.CancelJobRunInput{
		Id:               aws.String(jobID),
		VirtualClusterId: aws.String(virtualClusterID),
	}); err != nil {
		return "", fmt.Errorf("cancel-job-run %s/%s: %w", virtualClusterID, jobID, err)
	}
	return Cancelled, nil
}
