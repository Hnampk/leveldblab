package usecase1

import (
	"fmt"
	"leveldblab/db"
	"log"
	"math/rand"
	"os"
	"path"
	"sync"
	"time"
)

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

type message struct {
	key   int
	value string
}

func LevelDBMainTempTesting(read int, write int, duration time.Duration) {

	envRootFolder := os.Getenv("RootFolder")
	if envRootFolder != "" {
		rootFolder = envRootFolder + ""
	}
	dbFile, err := db.NewDB(path.Join(rootFolder, "usecase1"))
	if err != nil {
		log.Fatalf("error while create NewDB on path %s: %s", rootFolder, err.Error())
	}

	startTime := time.Now()
	channelWrite := make(chan *message, 1000)
	idx := 0
	// write
	var wg sync.WaitGroup
	for i := 0; i < write; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for msg := range channelWrite {
				key := fmt.Sprintf("TRANSACTION-%d", msg.key)
				value := msg.value
				timePutStart := time.Now()
				err := dbFile.Put(key, []byte(value))
				if err != nil {
					log.Printf("Error put key %s, err: %s\n", key, err.Error())
				}
				durationPut := time.Since(timePutStart).Milliseconds()
				if durationPut > 100 {
					log.Printf("Put slow key %s, duration %dms\n", key, durationPut)
				}
			}
		}()
	}

	// read
	count := 0
	var mx sync.Mutex
	for i := 0; i < read; i++ {
		go func() {
			for {
				mx.Lock()
				count++
				mx.Unlock()
				rand.Seed(time.Now().UnixNano())
				key := fmt.Sprintf("TRANSACTION-%d", rand.Intn(idx+1))
				_, err := dbFile.Get(key)
				if err != nil {
					// log.Printf("Error get key %s, err: %s\n", key, err.Error())
				}
			}
		}()
	}

	for time.Since(startTime) < duration {
		channelWrite <- &message{
			key:   idx,
			value: randStringBytes(60),
		}
		idx++
	}
	close(channelWrite)
	wg.Wait()
	log.Printf("Key write number: %d\n", idx)
	log.Printf("Key read number: %d\n", count)
}

func randStringBytes(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}
