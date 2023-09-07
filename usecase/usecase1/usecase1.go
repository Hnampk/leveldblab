package usecase1

import (
	"fmt"
	"leveldblab/config"
	"leveldblab/db"
	"log"
	"os"
	"strconv"
	"sync"
	"time"
)

const (
	latestKey = "latest"
)

var (
	myDB *db.LevelDBManager

	rootFolder = "./data"
)

func MainTempTesting() {
	envRootFolder := os.Getenv("RootFolder")
	if envRootFolder != "" {
		rootFolder = envRootFolder + ""
	}

	var err error
	myDB, err = db.NewDB(rootFolder)
	if err != nil {
		log.Fatalf("error while create NewDB on path %s: %s", rootFolder, err.Error())
	}

	start := time.Now()
	total := 10000000
	checkKeysOnInit(myDB)
	log.Printf("checkKeysOnInit done after %dms\n", time.Since(start).Milliseconds())

	if config.EnableWriting {
		log.Println("=== Start writing ===")
		// for i := 0; i < 9; i++ {
		// 	go func(index int) {
		// 		currentValue := getLatestKey(myDB, index)
		// 		for {
		// 			currentValue++
		// 			newKey := fmt.Sprintf("_%d%s", index, strconv.Itoa(currentValue))
		// 			if err := myDB.Put(newKey, []byte(fmt.Sprintf("value of %d", currentValue))); err != nil {
		// 				log.Printf("error while put value: %s", err.Error())
		// 			}
		// 			newLastKey := fmt.Sprintf("%d%s", index, latestKey)
		// 			if err := myDB.Put(newLastKey, []byte(strconv.Itoa(currentValue))); err != nil {
		// 				log.Printf("error while put value: %s", err.Error())
		// 			}
		// 		}
		// 	}(i)
		// }

		index := 9
		currentValue := getLatestKey(myDB, index)
		for i := 0; i < total; i++ {
			currentValue++
			newKey := fmt.Sprintf("_%d%s", index, strconv.Itoa(currentValue))
			if err := myDB.Put(newKey, []byte(fmt.Sprintf("value of %d", currentValue))); err != nil {
				log.Printf("error while put value: %s", err.Error())
			}
			newLastKey := fmt.Sprintf("%d%s", index, latestKey)
			if err := myDB.Put(newLastKey, []byte(strconv.Itoa(currentValue))); err != nil {
				log.Printf("error while put value: %s", err.Error())
			}
		}
	}
	log.Printf("write %d keys done after %dms", total, time.Since(start).Milliseconds())
}

func getLatestKey(db *db.LevelDBManager, index int) int {
	value, err := db.Get(fmt.Sprintf("%d%s", index, latestKey))
	if err != nil {
		return 0
	}

	latest, _ := strconv.Atoi(string(value))
	return latest
}

func checkKeysOnInit(db *db.LevelDBManager) bool {
	wg := &sync.WaitGroup{}
	for j := 0; j < 10; j++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			lastest := getLatestKey(db, index)
			log.Println("value while start: ", lastest)

			for i := 1; i < lastest; i++ {
				key := fmt.Sprintf("_%d%s", index, strconv.Itoa(i))
				value, err := db.Get(key)
				if err != nil {
					log.Printf("error while checkKeysOnInit with key %s: %s", key, err.Error())
					continue
				}
				valueStr := string(value)
				if valueStr != fmt.Sprintf("value of %d", i) {
					log.Fatalf("value of key %s unmatch. Got: %s", key, valueStr)
				}
			}
		}(j)
	}
	wg.Wait()

	log.Println("checkKeysOnInit successfully")

	return true
}
