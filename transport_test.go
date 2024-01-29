package raft

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfigurationEncoderDecoder(t *testing.T) {
	configuration := &Configuration{
		Members: map[string]string{"1": "127.0.0.0:8080", "2": "127.0.0.1:8080"},
		IsVoter: map[string]bool{"1": true, "2": false},
	}

	transport := &transport{}
	encodedConfiguration, err := transport.EncodeConfiguration(configuration)
	require.NoError(t, err)

	decodedConfiguration, err := transport.DecodeConfiguration(encodedConfiguration)
	require.NoError(t, err)

	require.Equal(t, configuration, &decodedConfiguration)
}
