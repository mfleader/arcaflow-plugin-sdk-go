package atp_test

import (
	"context"
	"fmt"
	"go.arcalot.io/assert"
	log "go.arcalot.io/log/v2"
	"go.flow.arcalot.io/pluginsdk/atp"
	"go.flow.arcalot.io/pluginsdk/schema"
	"golang.org/x/sync/errgroup"
	"io"
	"testing"
)

type helloWorldInput struct {
	Name string `json:"name"`
}

type helloWorldOutput struct {
	Message string `json:"message"`
}

func helloWorldHandler(input helloWorldInput) (string, any) {
	return "success", helloWorldOutput{
		Message: fmt.Sprintf("Hello, %s!", input.Name),
	}
}

var helloWorldSchema = schema.NewCallableSchema(
	schema.NewCallableStep[helloWorldInput](
		"hello-world",
		schema.NewScopeSchema(
			schema.NewStructMappedObjectSchema[helloWorldInput](
				"Input",
				map[string]*schema.PropertySchema{
					"name": schema.NewPropertySchema(
						schema.NewStringSchema(nil, nil, nil),
						nil,
						true,
						nil,
						nil,
						nil,
						nil,
						nil,
					),
				},
			),
		),
		map[string]*schema.StepOutputSchema{
			"success": schema.NewStepOutputSchema(
				schema.NewScopeSchema(
					schema.NewStructMappedObjectSchema[helloWorldOutput](
						"Output",
						map[string]*schema.PropertySchema{
							"message": schema.NewPropertySchema(
								schema.NewStringSchema(nil, nil, nil),
								nil,
								true,
								nil,
								nil,
								nil,
								nil,
								nil,
							),
						},
					),
				),
				nil,
				false,
			),
		},
		nil,
		helloWorldHandler,
	),
)

type channel struct {
	io.Reader
	io.Writer
	context.Context
	cancel func()
}

func (c channel) Close() error {
	c.cancel()
	return nil
}

func TestProtocol_Client_Execute(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	wg, gctx := errgroup.WithContext(ctx)
	stdinReader, stdinWriter := io.Pipe()
	stdoutReader, stdoutWriter := io.Pipe()

	wg.Go(func() error {
		return atp.RunATPServer(
			gctx,
			stdinReader,
			stdoutWriter,
			helloWorldSchema,
		)
	})
	wg.Go(func() error {
		defer cancel()
		cli := atp.NewClientWithLogger(channel{
			Reader:  stdoutReader,
			Writer:  stdinWriter,
			Context: ctx,
			cancel:  cancel,
		}, log.NewTestLogger(t))

		_, err := cli.ReadSchema()
		if err != nil {
			return err
		}

		outputID, outputData, err := cli.Execute(ctx, "hello-world", map[string]any{"name": "Arca Lot"})
		if err != nil {
			return err
		}
		if outputID != "success" {
			return fmt.Errorf("Invalid output ID: %s", outputID)
		}
		if outputMessage := outputData.(map[any]any)["message"].(string); outputMessage != "Hello, Arca Lot!" {
			return fmt.Errorf("Invalid output message: %s", outputMessage)
		}
		return nil
	})

	assert.NoError(t, wg.Wait())
}

func TestProtocol_Client_ReadSchema(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	wg, gctx := errgroup.WithContext(ctx)
	stdinReader, stdinWriter := io.Pipe()
	stdoutReader, stdoutWriter := io.Pipe()

	wg.Go(func() error {
		return atp.RunATPServer(
			gctx,
			stdinReader,
			stdoutWriter,
			helloWorldSchema,
		)
	})

	wg.Go(func() error {
		// terminate the protocol execution
		// because it will not be completed
		defer cancel()

		cli := atp.NewClientWithLogger(channel{
			Reader:  stdoutReader,
			Writer:  stdinWriter,
			Context: nil,
			cancel:  cancel,
		}, log.NewTestLogger(t))
		_, err := cli.ReadSchema()
		return err
	})

	assert.NoError(t, wg.Wait())
}

func TestProtocol_Error_Client_Output(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	wg, gctx := errgroup.WithContext(ctx)
	stdinReader, stdinWriter := io.Pipe()
	stdoutReader, stdoutWriter := io.Pipe()

	wg.Go(func() error {
		return atp.RunATPServer(
			gctx,
			stdinReader,
			stdoutWriter,
			helloWorldSchema,
		)
	})
	wg.Go(func() error {
		cli := atp.NewClientWithLogger(channel{
			Reader:  stdoutReader,
			Writer:  stdinWriter,
			Context: nil,
			cancel:  cancel,
		}, log.NewTestLogger(t))

		assert.NoError(t, stdinReader.Close())

		_, err := cli.ReadSchema()
		return err
	})

	assert.Error(t, wg.Wait())
}

func TestProtocol_Error_Client_Hello_ClosedPipe(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	wg, gctx := errgroup.WithContext(ctx)
	stdinReader, stdinWriter := io.Pipe()
	stdoutReader, stdoutWriter := io.Pipe()

	wg.Go(func() error {
		return atp.RunATPServer(
			gctx,
			stdinReader,
			stdoutWriter,
			helloWorldSchema,
		)
	})
	wg.Go(func() error {
		cli := atp.NewClientWithLogger(channel{
			Reader:  stdoutReader,
			Writer:  stdinWriter,
			Context: nil,
			cancel:  cancel,
		}, log.NewTestLogger(t))

		assert.NoError(t, stdoutReader.Close())

		_, err := cli.ReadSchema()
		return err
	})

	assert.Error(t, wg.Wait())
}

func TestProtocol_Error_Client_Hello_EOF(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	wg, gctx := errgroup.WithContext(ctx)
	stdinReader, stdinWriter := io.Pipe()
	stdoutReader, stdoutWriter := io.Pipe()

	wg.Go(func() error {
		return atp.RunATPServer(
			gctx,
			stdinReader,
			stdoutWriter,
			helloWorldSchema,
		)
	})
	wg.Go(func() error {
		cli := atp.NewClientWithLogger(channel{
			Reader:  stdoutReader,
			Writer:  stdinWriter,
			Context: nil,
			cancel:  cancel,
		}, log.NewTestLogger(t))

		assert.NoError(t, stdoutWriter.Close())

		_, err := cli.ReadSchema()
		return err
	})

	assert.Error(t, wg.Wait())
}

func TestProtocol_Error_Server(t *testing.T) {
	stdinReader, _ := io.Pipe()
	_, stdoutWriter := io.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	assert.NoError(t, stdinReader.Close())
	assert.NoError(t, stdoutWriter.Close())

	assert.Error(t, atp.RunATPServer(
		ctx,
		stdinReader,
		stdoutWriter,
		helloWorldSchema,
	))
}
