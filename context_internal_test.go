package tchannel

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewContextBuilderDisableTracing(t *testing.T) {
	ctx, cancel := NewContextBuilder(time.Second).
		DisableTracing().Build()
	defer cancel()

	assert.True(t, isTracingDisabled(ctx), "Tracing should be disabled")
}
