package pure

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/tarantool/go-gostcrypto/streebog"
	"github.com/warpstreamlabs/bento/public/service"
)

const (
	gostHashCheckDataPathField = "data_path"

	gostHashCheckMetaValid     = "hashValid"
	gostHashCheckMetaComputed  = "computedHash"
	gostHashCheckMetaProvided  = "providedHash"
	gostHashCheckMetaAlgorithm = "hashAlgorithm"

	gostHashCheckAlgorithm = "GOST R 34.11-2012-256"
)

func init() {
	err := service.RegisterBatchProcessor(
		"gost_hash_check",
		service.NewConfigSpec().
			Categories("Utility", "Processing").
			Stable().
			Summary("Validates message hashes using GOST R 34.11-2012-256 (Streebog-256).").
			Description(`
Extracts the message and expected hash from a JSON object and validates
the hash using GOST R 34.11-2012-256.

The original message body is preserved. Results are written to message
metadata: hashValid, computedHash, providedHash and hashAlgorithm.
Malformed messages are marked as processing errors individually.
`).
			Fields(
				service.NewStringField(gostHashCheckDataPathField).
					Description("JSON path to the object containing message and hash fields.").
					Default("payload"),
			),
		func(conf *service.ParsedConfig, _ *service.Resources) (service.BatchProcessor, error) {
			dataPath, err := conf.FieldString(gostHashCheckDataPathField)
			if err != nil {
				return nil, err
			}
			dataPath = strings.TrimSpace(dataPath)
			if dataPath == "" {
				return nil, errors.New("data_path must not be empty")
			}

			return &gostHashCheckProcessor{
				dataPath: strings.Split(dataPath, "."),
			}, nil
		},
	)
	if err != nil {
		panic(err)
	}
}

type gostHashCheckProcessor struct {
	dataPath []string
}

func (g *gostHashCheckProcessor) ProcessBatch(
	ctx context.Context,
	batch service.MessageBatch,
) ([]service.MessageBatch, error) {
	for _, msg := range batch {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		g.processMessage(msg)
	}
	return []service.MessageBatch{batch}, nil
}

func (g *gostHashCheckProcessor) processMessage(msg *service.Message) {
	msg.MetaDelete(gostHashCheckMetaValid)
	msg.MetaDelete(gostHashCheckMetaComputed)
	msg.MetaDelete(gostHashCheckMetaProvided)
	msg.MetaDelete(gostHashCheckMetaAlgorithm)

	document, err := msg.AsStructured()
	if err != nil {
		msg.SetError(fmt.Errorf("decode message JSON: %w", err))
		return
	}

	root, ok := document.(map[string]any)
	if !ok {
		msg.SetError(errors.New("message must be a JSON object"))
		return
	}

	data := root
	for _, key := range g.dataPath {
		value, exists := data[key]
		if !exists {
			msg.SetError(fmt.Errorf("field %q not found", key))
			return
		}

		next, ok := value.(map[string]any)
		if !ok {
			msg.SetError(fmt.Errorf("field %q must be a JSON object", key))
			return
		}
		data = next
	}

	messageValue, ok := data["message"].(string)
	if !ok {
		msg.SetError(fmt.Errorf("field %q.message must be a string", strings.Join(g.dataPath, ".")))
		return
	}

	providedHash, ok := data["hash"].(string)
	if !ok {
		msg.SetError(fmt.Errorf("field %q.hash must be a string", strings.Join(g.dataPath, ".")))
		return
	}

	decodedHash, err := hex.DecodeString(providedHash)
	if err != nil || len(decodedHash) != 32 {
		msg.SetError(errors.New("provided hash must be a valid 256-bit hexadecimal value"))
		return
	}

	sum := streebog.Sum256([]byte(messageValue))
	computedHash := hex.EncodeToString(sum[:])

	msg.MetaSetMut(gostHashCheckMetaValid, strings.EqualFold(computedHash, providedHash))
	msg.MetaSet(gostHashCheckMetaComputed, computedHash)
	msg.MetaSet(gostHashCheckMetaProvided, providedHash)
	msg.MetaSet(gostHashCheckMetaAlgorithm, gostHashCheckAlgorithm)
}

func (g *gostHashCheckProcessor) Close(context.Context) error {
	return nil
}
