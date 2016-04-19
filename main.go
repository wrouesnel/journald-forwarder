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
	"io/ioutil"
	"time"
	"encoding/binary"
	"net/url"
	"net"
"encoding/json"
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

var (
	lastTimestamp uint64
)

func main() {
	var err error
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

	// Forward messages
	var conn net.Conn
	for {
		// Loop endlessly to try and reconnect
		for {
			conn, err = net.Dial(proto, host)
			if err == nil {
				break
			}
			log.Errorln("Failed to connect to remote host, retrying. Error: ", err)
			time.Sleep(*networkRetry)
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
		}
	}
}