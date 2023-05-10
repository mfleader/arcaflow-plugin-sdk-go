package atp

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/fxamacker/cbor/v2"
	"go.flow.arcalot.io/pluginsdk/schema"
)

// RunATPServer runs an ArcaflowTransportProtocol server with a given schema.
func RunATPServer( //nolint:funlen
	ctx context.Context,
	stdin io.ReadCloser,
	stdout io.WriteCloser,
	s *schema.CallableSchema,
) error {
	wg := &sync.WaitGroup{}
	wg.Add(2)
	workDone := make(chan struct{})
	errch := make(chan error, 1)
	var errchError error
	//closed := false // If this becomes a problem, switch this out for an atomic bool.
	go func() {
		defer wg.Done()
		select {
		case errchError = <-errch:
			//closed = true
			_ = stdin.Close()
		case <-ctx.Done():
			// The context is closed! That means this was instructed to stop.
			// First let the code know that it was closed so that it doesn't panic.
			//closed = true
			//closed <- struct{}{}
			// Now close the pipe that it gets input from.
			_ = stdin.Close()
		case <-workDone:
			// Done, so let it move on to the deferred wg.Done
		}
	}()

	go func() {
		defer wg.Done()
		defer close(workDone)
		defer close(errch)
		//defer stdin.Close()

		var serializedSchema any
		var err error
		var cborStdin *cbor.Decoder
		var cborStdout *cbor.Encoder

		// Start by serializing the schema, since the protocol requires sending the schema on the hello message.
		//select {
		//case <-ctx.Done():
		//	_ = stdin.Close()
		//	return
		//default:
		//	serializedSchema, err = s.SelfSerialize()
		//	if err != nil {
		//		errch <- err
		//		return
		//	}
		//}
		serializedSchema, err = s.SelfSerialize()
		if err != nil {
			errch <- err
			return
		}

		// The ATP protocol uses CBOR.
		//select {
		//case <-ctx.Done():
		//	_ = stdin.Close()
		//	return
		//default:
		//	cborStdin = cbor.NewDecoder(stdin)
		//	cborStdout = cbor.NewEncoder(stdout)
		//}
		cborStdin = cbor.NewDecoder(stdin)
		cborStdout = cbor.NewEncoder(stdout)

		// stop if closed

		// First, the start message, which is just an empty message.
		//select {
		//case <-ctx.Done():
		//	_ = stdin.Close()
		//	return
		//default:
		//	var empty any
		//	err = cborStdin.Decode(&empty)
		//	if err != nil {
		//		errch <- fmt.Errorf("failed to CBOR-decode start output message (%w)", err)
		//		return
		//	}
		//}
		var empty any
		err = cborStdin.Decode(&empty)
		if err != nil {
			errch <- fmt.Errorf("failed to CBOR-decode start output message (%w)", err)
			return
		}

		// Next, send the hello message, which includes the version and schema.
		//select {
		//case <-ctx.Done():
		//	//_ = stdin.Close()
		//	return
		//default:
		//	err = cborStdout.Encode(HelloMessage{1, serializedSchema})
		//	if err != nil {
		//		errch <- fmt.Errorf("failed to CBOR-encode schema (%w)", err)
		//		return
		//	}
		//}
		err = cborStdout.Encode(HelloMessage{1, serializedSchema})
		if err != nil {
			errch <- fmt.Errorf("failed to CBOR-encode schema (%w)", err)
			return
		}

		// Now, get the work message that dictates which step to run and the config info.
		req := StartWorkMessage{}
		//select {
		//case <-ctx.Done():
		//	//_ = stdin.Close()
		//	return
		//default:
		//	err = cborStdin.Decode(&req)
		//	if err != nil {
		//		errch <- fmt.Errorf("failed to CBOR-decode start work message (%w)", err)
		//		return
		//	}
		//}
		err = cborStdin.Decode(&req)
		if err != nil {
			errch <- fmt.Errorf("failed to CBOR-decode start work message (%w)", err)
			return
		}

		// stop if closed
		var outputID string
		var outputData any
		//select {
		//case <-ctx.Done():
		//	//_ = stdin.Close()
		//	return
		//default:
		//	outputID, outputData, err = s.Call(req.StepID, req.Config)
		//	if err != nil {
		//		errch <- err
		//		return
		//	}
		//}
		outputID, outputData, err = s.Call(req.StepID, req.Config)
		if err != nil {
			errch <- err
			return
		}

		//Lastly, send the work done message.
		//select {
		//case <-ctx.Done():
		//	_ = stdin.Close()
		//	return
		//default:
		//	err = cborStdout.Encode(workDoneMessage{
		//		outputID,
		//		outputData,
		//		"",
		//	})
		//	if err != nil {
		//		errch <- fmt.Errorf("failed to encode CBOR response (%w)", err)
		//		return
		//	}
		//}
		err = cborStdout.Encode(workDoneMessage{
			outputID,
			outputData,
			"",
		})
		if err != nil {
			errch <- fmt.Errorf("failed to encode CBOR response (%w)", err)
			return
		}
	}()

	// Keep running until both goroutines are done
	wg.Wait()
	return errchError
}
