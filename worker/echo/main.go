package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"

	"git.sr.ht/~spc/go-log"

	"github.com/redhatinsights/yggdrasil/internal/sync"
	"github.com/redhatinsights/yggdrasil/ipc"
	"github.com/redhatinsights/yggdrasil/worker"
)

var sleepTime time.Duration

// sycMapCancelChan is a map of channels that maps message ID and its current
// work. This allows for the cancellation of a message that has not finished.
var sycMapCancelChan sync.RWMutexMap[chan struct{}]

// echo opens a new dbus connection and calls the
// com.redhat.Yggdrasil1.Dispatcher1.Transmit method, returning the
// metadata and data. New ID is generated for the message, and
// response_to is set to the ID of the message we received.
// Cancelled messages will only be handled if the worker is running
// in the slow mode.
func echo(
	w *worker.Worker,
	addr string,
	rcvId string,
	responseTo string,
	metadata map[string]string,
	data []byte,
) error {
	if err := w.EmitEvent(ipc.WorkerEventNameWorking, rcvId, fmt.Sprintf("echoing %v", data)); err != nil {
		return fmt.Errorf("cannot call EmitEvent: %w", err)
	}

	// Sleep time between receiving the message and sending it
	if sleepTime > 0 {
		// Setting the channel to handle cancellation
		sycMapCancelChan.Set(rcvId, make(chan struct{}))

		log.Tracef("sleeping: %v", sleepTime)
		time.Sleep(sleepTime)

		// Cancel message if it has been sent a cancel message
		// during sleep time
		cancelChan, _ := sycMapCancelChan.Get(rcvId)
		select {
		case <-cancelChan:
			log.Tracef("canceled echo message id: %v", rcvId)
			sycMapCancelChan.Del(rcvId)
			return nil
		default:
			sycMapCancelChan.Del(rcvId)
		}
	}

	// Set "response_to" according to the rcvId of the message we received
	echoResponseTo := rcvId
	// Create new echoId for the message we are going to send
	echoId := uuid.New().String()

	responseCode, responseMetadata, responseData, err := w.Transmit(
		addr,
		echoId,
		echoResponseTo,
		metadata,
		data,
	)
	if err != nil {
		return fmt.Errorf("cannot call Transmit: %w", err)
	}

	// Log the responses received from the Dispatcher, if any.
	log.Infof("responseCode = %v", responseCode)
	log.Infof("responseMetadata = %#v", responseMetadata)
	log.Infof("responseData = %v", responseData)

	if err := w.SetFeature("DispatchedAt", time.Now().Format(time.RFC3339)); err != nil {
		return fmt.Errorf("cannot set feature: %w", err)
	}

	return nil
}

// cancelEcho receives a cancel message id via  com.redhat.Yggdrasil1.Worker1.C.ancel method
// closes the channel associated to that message to cancel its current run
func cancelEcho(w *worker.Worker, addr string, id string, cancelID string) error {
	log.Infof("cancelling message with id %v", cancelID)
	if cancelChan, exists := sycMapCancelChan.Get(cancelID); exists {
		close(cancelChan)
	}

	return nil
}

func events(event ipc.DispatcherEvent) {
	switch event {
	case ipc.DispatcherEventReceivedDisconnect:
		os.Exit(1)
	}
}

func main() {
	var (
		logLevel      string
		remoteContent bool
	)

	flag.StringVar(&logLevel, "log-level", "error", "set log level")
	flag.BoolVar(&remoteContent, "remote-content", false, "connect as a remote content worker")
	flag.DurationVar(&sleepTime, "sleep", 0, "sleep time in seconds before echoing the response")
	flag.Parse()

	level, err := log.ParseLevel(logLevel)
	if err != nil {
		log.Fatalf("error: cannot parse log level: %v", err)
	}
	log.SetLevel(level)

	w, err := worker.NewWorker(
		"echo",
		remoteContent,
		map[string]string{"DispatchedAt": "", "Version": "1"},
		cancelEcho,
		echo,
		events,
	)
	if err != nil {
		log.Fatalf("error: cannot create worker: %v", err)
	}

	// Set up a channel to receive the TERM or INT signal over and clean up
	// before quitting.
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)

	if err := w.Connect(quit); err != nil {
		log.Fatalf("error: cannot connect: %v", err)
	}
}
