// Package probe reads the last activity of a pod from the Kubernetes API.
package probe

import (
	"context"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
)

// LogProbe reports when a container last wrote a log line.
type LogProbe interface {
	// LastLogTime returns the timestamp of the newest log line. ok is false
	// when the container has not logged anything yet.
	LastLogTime(ctx context.Context, namespace, pod, container string) (t time.Time, ok bool, err error)
}

// KubeLogProbe implements LogProbe using pods/log (kubelet-timestamped lines).
type KubeLogProbe struct {
	Client kubernetes.Interface
}

func (p *KubeLogProbe) LastLogTime(ctx context.Context, namespace, pod, container string) (time.Time, bool, error) {
	tail := int64(1)
	raw, err := p.Client.CoreV1().Pods(namespace).GetLogs(pod, &corev1.PodLogOptions{
		Container:  container,
		Timestamps: true,
		TailLines:  &tail,
	}).DoRaw(ctx)
	if err != nil {
		return time.Time{}, false, fmt.Errorf("reading logs of %s/%s: %w", namespace, pod, err)
	}
	return ParseLastTimestamp(string(raw))
}

// ParseLastTimestamp extracts the RFC3339Nano prefix that the kubelet adds to
// each line when Timestamps=true.
func ParseLastTimestamp(out string) (time.Time, bool, error) {
	out = strings.TrimSpace(out)
	if out == "" {
		return time.Time{}, false, nil
	}
	lines := strings.Split(out, "\n")
	line := lines[len(lines)-1]
	tok, _, _ := strings.Cut(line, " ")
	t, err := time.Parse(time.RFC3339Nano, tok)
	if err != nil {
		return time.Time{}, false, fmt.Errorf("parsing log timestamp %q: %w", tok, err)
	}
	return t, true, nil
}
