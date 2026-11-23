package pure_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/internal/component/processor"
	"github.com/warpstreamlabs/bento/internal/component/testutil"
	"github.com/warpstreamlabs/bento/internal/manager/mock"
	"github.com/warpstreamlabs/bento/internal/message"
)

const (
	gostTestMessage = "Тестовое сообщение для конектора kafka"
	gostTestHash    = "32b4ef0fa916e09864275ab3f4b927b869703b79ae9accdc3a7e49fc8445c44c"
)

func newGostHashCheckProcessor(t *testing.T, config string) processor.V1 {
	t.Helper()

	conf, err := testutil.ProcessorFromYAML(config)
	require.NoError(t, err)

	proc, err := mock.NewManager().NewProcessor(conf)
	require.NoError(t, err)

	return proc
}

func TestGostHashCheckValid(t *testing.T) {
	proc := newGostHashCheckProcessor(t, `
gost_hash_check: {}
`)

	input := []byte(`{"schema":{"type":"struct"},"payload":{"message":"Тестовое сообщение для конектора kafka","hash":"32b4ef0fa916e09864275ab3f4b927b869703b79ae9accdc3a7e49fc8445c44c"}}`)
	batch := message.QuickBatch([][]byte{input})

	output, err := proc.ProcessBatch(context.Background(), batch)
	require.NoError(t, err)
	require.Len(t, output, 1)
	require.Len(t, output[0], 1)

	part := output[0][0]
	require.NoError(t, part.ErrorGet())

	valid, exists := part.MetaGetMut("hashValid")
	require.True(t, exists)
	require.Equal(t, true, valid)

	computed, exists := part.MetaGetMut("computedHash")
	require.True(t, exists)
	require.Equal(t, gostTestHash, computed)

	provided, exists := part.MetaGetMut("providedHash")
	require.True(t, exists)
	require.Equal(t, gostTestHash, provided)

	algorithm, exists := part.MetaGetMut("hashAlgorithm")
	require.True(t, exists)
	require.Equal(t, "GOST R 34.11-2012-256", algorithm)

	actual := part.AsBytes()
	require.JSONEq(t, string(input), string(actual))
}

func TestGostHashCheckCaseInsensitive(t *testing.T) {
	proc := newGostHashCheckProcessor(t, `
gost_hash_check: {}
`)

	input := []byte(`{"payload":{"message":"Тестовое сообщение для конектора kafka","hash":"` + strings.ToUpper(gostTestHash) + `"}}`)
	output, err := proc.ProcessBatch(context.Background(), message.QuickBatch([][]byte{input}))
	require.NoError(t, err)

	valid, exists := output[0][0].MetaGetMut("hashValid")
	require.True(t, exists)
	require.Equal(t, true, valid)
}

func TestGostHashCheckMismatch(t *testing.T) {
	proc := newGostHashCheckProcessor(t, `
gost_hash_check: {}
`)

	input := []byte(`{"payload":{"message":"Тестовое сообщение для конектора kafka","hash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}`)
	output, err := proc.ProcessBatch(context.Background(), message.QuickBatch([][]byte{input}))
	require.NoError(t, err)

	part := output[0][0]
	require.NoError(t, part.ErrorGet())

	valid, exists := part.MetaGetMut("hashValid")
	require.True(t, exists)
	require.Equal(t, false, valid)
}

func TestGostHashCheckErrorsArePerMessage(t *testing.T) {
	proc := newGostHashCheckProcessor(t, `
gost_hash_check: {}
`)

	inputs := [][]byte{
		[]byte(`{"payload":{"message":"Тестовое сообщение для конектора kafka","hash":"` + gostTestHash + `"}}`),
		[]byte(`{"payload":{"message":42,"hash":"` + gostTestHash + `"}}`),
		[]byte(`not-json`),
	}

	output, err := proc.ProcessBatch(context.Background(), message.QuickBatch(inputs))
	require.NoError(t, err)
	require.Len(t, output, 1)
	require.Len(t, output[0], 3)

	require.NoError(t, output[0][0].ErrorGet())
	require.Error(t, output[0][1].ErrorGet())
	require.Error(t, output[0][2].ErrorGet())

	valid, exists := output[0][0].MetaGetMut("hashValid")
	require.True(t, exists)
	require.Equal(t, true, valid)

	// Failed messages must not be classified as a normal hash mismatch.
	_, exists = output[0][1].MetaGetMut("hashValid")
	require.False(t, exists)
	_, exists = output[0][2].MetaGetMut("hashValid")
	require.False(t, exists)
}

func TestGostHashCheckCustomDataPath(t *testing.T) {
	proc := newGostHashCheckProcessor(t, `
gost_hash_check:
  data_path: record.data
`)

	payload := map[string]any{
		"record": map[string]any{
			"data": map[string]any{
				"message": gostTestMessage,
				"hash":    gostTestHash,
			},
		},
	}
	input, err := json.Marshal(payload)
	require.NoError(t, err)

	output, err := proc.ProcessBatch(context.Background(), message.QuickBatch([][]byte{input}))
	require.NoError(t, err)

	valid, exists := output[0][0].MetaGetMut("hashValid")
	require.True(t, exists)
	require.Equal(t, true, valid)
}
