/*
	This is a simple JSON based journald-forwarder, based on the CoreOS
	golang bindings to sdjournal. It implements a simple checkpointing resume
	mechanism for streaming logs, allowing recovery in event of an unclean
	shutdown.
 */

package main

import (
	"flag"
	"github.com/wrouesnel/go.log"
	"github.com/coreos/go-systemd/sdjournal"
	"os"
	"time"
	"encoding/binary"
	"net/url"
	"net"
	"encoding/json"
	"os/signal"
)

const (
	GitCommit = "unknown"
	Version = "0.0.0-dev"
	DefaultStateFile = "journald-forwarder.state"

)

var (
	remote = flag.String("remote", "", "Remote target for forwarding logs (e.g. tcp://127.0.0.1:514")
	stateFile = flag.String("statefile", DefaultStateFile, "File to checkpoint log position to for resuming.")
	checkpointInterval = flag.Duration("checkpoint", time.Second * 5, "Time between checkpoints for sent logs. Default 5 seconds.")
	networkRetry = flag.Duration("network-retry", time.Second, "Time between attempting reconnects to a network endpoint. Default 1 second.")
)

func main() {
	var err error
	var lastTimestamp uint64
	flag.Parse()

	// Parse the remote in advance.
	remote, err := url.Parse(*remote)
	if err != nil {
		log.Fatalln("Could not parse remote address.")
	}
	proto := remote.Scheme
	host := remote.Host

	if *stateFile == "" {
		log.Warnln("Running with out a state file. Will send logs from now.")
	} else {
		fd, err := os.Open(*stateFile)
		if err != nil {
			log.Warnln("State file could not be opened. Sending logs from beginning. Error:", err)
		} else {
			binary.Read(fd, binary.LittleEndian, &lastTimestamp)
		}
		fd.Close()
	}

	// Open the journal
	j, err := sdjournal.NewJournal()
	if err != nil {
		log.Fatalln("Could not open journal. Error:", err)
	}

	// We want to seek to the first entry on or just before the last timestamp
	// we remember sending.
	log.Infoln("Recovering log position: ", lastTimestamp)
	for {
		// Try and seek an entry
		r, err := j.Next()
		if err != nil {
			log.Errorln("Error advancing journal during recovery: jumping to end.")
			err := j.SeekTail()
			if err != nil {
				log.Fatalln("Error seeking to journal tail during recovery. Giving up and exiting.")
			}
			break
		}

		// Did we not find any timestamps we want?
		if r == 0 {
			log.Infoln("Found end of journal while recovering position. Empty journal?")
			break
		}

		// Compare this entry with the last we sent. If bigger, start from here.
		rt, err := j.GetRealtimeUsec()
		if err == nil {
			if rt > lastTimestamp {
				log.Infoln("Next log position is:", rt)
				break
			}
		}
		// Continue seeking otherwise.
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)

	checkticker := time.Tick(*checkpointInterval)

	// Forward messages
	var conn net.Conn
	MainLoop: for {
		// Loop endlessly to try and reconnect
		for {
			conn, err = net.Dial(proto, host)
			if err == nil {
				break
			}
			log.Errorln("Failed to connect to remote host, retrying. Error: ", err)
			sleepCh := time.After(*networkRetry)
			select {
			case _ = <- sigCh:
				break MainLoop
			case _ = <- sleepCh:
			}
		}

		// Connected, start sending entries.
		for {
			data, err := j.GetDataMap()
			b, err := json.Marshal(&data)
			if err != nil {
				log.Fatalln("Error encoding JSON (BUG):", err)
			}

			_, err = conn.Write(b)
			if err != nil {
				log.Infoln("Connection error, reconnecting:", err)
				break	// Loop back to reconnect
			}
			lastTimestamp, err = j.GetRealtimeUsec()
			if err != nil {
				log.Errorln("Error getting the timestamp of the sent entry")
			}

			r, err := j.Next()
			if err != nil {
				// we could try re-opening and doing log recovery in the future
				log.Fatalln("Error while reading next journal entry. Exiting:", err)
			}

			if r == 0 {
				// We're at the end of the journal, so wait until a new entry
				// before continuing to loop.
				wait := make(chan interface{})
				go func(wait chan<- interface{}) {
					j.Wait(0)
					close(wait)
				}(wait)

				JournalWait: for {
					select {
					case _ = <-sigCh:
						break MainLoop
					case _ = <-checkticker:
						// Checkpoint the state file
						log.Debugln("Checkpointing at:", lastTimestamp)
						WriteStateFile(*stateFile, lastTimestamp)
					case _ = <-wait:
						break JournalWait
					}
				}
			}
		}
	}

	log.Infoln("Writing checkpoint before graceful exit")
	WriteStateFile(*stateFile, lastTimestamp)

	log.Infoln("Exiting normally.")
	os.Exit(0)
}

func WriteStateFile(stateFile string, lastTimestamp uint64) {
	fd, err := os.OpenFile(stateFile, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, os.FileMode(0777))
	defer fd.Close()	// We don't checkpoint often, and want to force sync.
	if err == nil {
		err = binary.Write(fd, binary.LittleEndian, &lastTimestamp)
		if err != nil {
			log.Errorln("Error writing state file:", err)
		}
	} else {
		log.Errorln("Error writing state file:", err)
	}
}