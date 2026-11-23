package service

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfigHTTPTransportMaxIdleConnsPerHostDefault(t *testing.T) {
	spec := NewConfigSpec().
		Field(NewTransportField("transport"))

	parsedConfig, err := spec.ParseYAML(`
transport: {}
`, nil)
	require.NoError(t, err)

	transport, err := parsedConfig.FieldHTTPTransport("transport")
	require.NoError(t, err)

	require.Equal(t, 2, transport.MaxIdleConnsPerHost)
}

func TestConfigHTTPTransportMaxConnsPerHostDefault(t *testing.T) {
	spec := NewConfigSpec().
		Field(NewTransportField("transport"))

	parsedConfig, err := spec.ParseYAML(`
transport: {}
`, nil)
	require.NoError(t, err)

	transport, err := parsedConfig.FieldHTTPTransport("transport")
	require.NoError(t, err)

	require.Equal(t, 0, transport.MaxConnsPerHost)
}

func TestConfigHTTPTransportMaxConnsPerHostCustom(t *testing.T) {
	spec := NewConfigSpec().
		Field(NewTransportField("transport"))

	parsedConfig, err := spec.ParseYAML(`
transport:
  max_connections_per_host: 48
`, nil)
	require.NoError(t, err)

	transport, err := parsedConfig.FieldHTTPTransport("transport")
	require.NoError(t, err)

	require.Equal(t, 48, transport.MaxConnsPerHost)
}

func TestConfigHTTPTransportMaxIdleConnsPerHostCustom(t *testing.T) {
	spec := NewConfigSpec().
		Field(NewTransportField("transport"))

	parsedConfig, err := spec.ParseYAML(`
transport:
  max_idle_connections_per_host: 512
`, nil)
	require.NoError(t, err)

	transport, err := parsedConfig.FieldHTTPTransport("transport")
	require.NoError(t, err)

	require.Equal(t, 512, transport.MaxIdleConnsPerHost)
}
