package db

import (
	"fmt"
	"leveldblab/config"
	"log"
	"sync"
	"time"

	cp "github.com/otiai10/copy"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

const (
	MsgBackup = iota
	MsgMerge
	MsgPut
	MsgGet
)

type Message struct {
	action int
	res    chan error

	key   string
	value []byte
}

type LevelDBManager struct {
	path     string
	msgQueue chan Message

	mainDB *levelDBWrapper
	tempDB *levelDBWrapper // use on Backup time
}

type levelDBWrapper struct {
	path string
	db   *leveldb.DB
	wg   *sync.WaitGroup
}

var (
	dbManagerInstance *LevelDBManager
	backupInterval    = 30 * time.Second

	wo = &opt.WriteOptions{
		NoWriteMerge: false,
		Sync:         false,
	}
)

func NewDB(path string) (*LevelDBManager, error) {
	log.Printf("Create newDB at path: %s", path)
	mainDB := &levelDBWrapper{
		path: path + "/main",
		wg:   &sync.WaitGroup{},
	}
	if err := mainDB.open(); err != nil {
		return nil, err
	}
	tempDB := &levelDBWrapper{
		path: path + "/temp",
		wg:   &sync.WaitGroup{},
	}
	if err := tempDB.open(); err != nil {
		return nil, err
	}

	db := &LevelDBManager{
		path:     path,
		msgQueue: make(chan Message),
		mainDB:   mainDB,
		tempDB:   tempDB,
	}
	go db.start()

	return db, nil
}

func (dw *levelDBWrapper) put(key, value []byte) error {
	return dw.db.Put(key, value, wo)
}
func (dw *levelDBWrapper) delete(key []byte) error {
	return dw.db.Delete(key, wo)
}

func (dw *levelDBWrapper) close() error {
	return dw.db.Close()
}

func (dw *levelDBWrapper) open() error {
	db, err := leveldb.OpenFile(dw.path, nil)
	if err != nil {
		log.Fatalf("Error load db folder %s: %v", dw.path, err)
		return err
	}

	dw.db = db
	return nil
}
func (dw *levelDBWrapper) setReadOnly() error {
	return dw.db.SetReadOnly()
}

func (dm *LevelDBManager) start() {
	workingDB := dm.mainDB
	go dm.triggerMergeDB()

	lastkey := ""
	for msg := range dm.msgQueue {
		request := msg
		switch request.action {
		case MsgPut:
			workingDB.wg.Add(1)
			go func(db *levelDBWrapper, request Message) {
				defer db.wg.Done()
				lastkey = request.key
				request.res <- db.put([]byte(request.key), request.value)
			}(workingDB, request)

		case MsgBackup:
			dm.mainDB.wg.Wait()
			workingDB = dm.tempDB

			go func() {
				// backup
				// if err := dm.mainDB.setReadOnly(); err != nil {
				// 	log.Printf("[catch me] error while SetReadOnly mainDB %s: %s", dm.mainDB.path, err.Error())
				// 	return
				// }

				if err := dm.mainDB.close(); err != nil {
					log.Printf("[catch me] error while close mainDB %s: %s", dm.mainDB.path, err.Error())
					return
				}

				start := time.Now()
				backupName := fmt.Sprintf("./backup-%d/%s", time.Now().Nanosecond(), dm.mainDB.path)
				log.Printf("Start Backup %s. last key: %s", backupName, lastkey)
				defer func(backupName string, start time.Time) {
					log.Printf("Backup %s done after %dms", backupName, time.Since(start).Milliseconds())
				}(backupName, start)

				if err := cp.Copy(dm.mainDB.path, backupName); err != nil {
					log.Printf("[catch me] error while Backup mainDB %s: %s", dm.mainDB.path, err.Error())
					return
				}
				if err := dm.mainDB.open(); err != nil {
					log.Printf("[catch me] error while reopen mainDB after backup %s: %s", dm.mainDB.path, err.Error())
					return
				}

				dm.triggerMergeDB()
			}()

		case MsgMerge:
			dm.tempDB.wg.Wait()
			workingDB = dm.mainDB

			go func() {
				// merge
				start := time.Now()
				log.Printf("Start Merge. last key: %s", lastkey)

				iter := dm.tempDB.db.NewIterator(util.BytesPrefix([]byte("")), nil)
				defer iter.Release()
				count := 0
				for iter.Next() {
					count++
					key := iter.Key()
					value := iter.Value()

					// TODO: check on state version before overwrite data
					if err := dm.mainDB.put(key, value); err != nil {
						continue
					} else {
						if err := dm.tempDB.delete(key); err != nil {
							continue
						}
					}
				}
				log.Printf("Merge %d keys done after %dms", count, time.Since(start).Milliseconds())

				if config.EnableBackup {
					<-time.After(backupInterval)
					dm.triggerBackupDB()
				}
			}()
		}
	}
}

func (dm *LevelDBManager) triggerMergeDB() {
	dm.msgQueue <- Message{
		action: MsgMerge,
	}
}

func (dm *LevelDBManager) triggerBackupDB() {
	dm.msgQueue <- Message{
		action: MsgBackup,
	}
}

func (dm *LevelDBManager) Put(key string, value []byte) error {
	res := make(chan error)
	dm.msgQueue <- Message{
		action: MsgPut,
		res:    res,
		key:    key,
		value:  value,
	}
	return <-res
}

func (dm *LevelDBManager) Get(key string) ([]byte, error) {
	value, err := dm.tempDB.db.Get([]byte(key), nil)
	if err != nil && err != leveldb.ErrNotFound {
		return nil, err
	}

	if len(value) > 0 {
		mainValue, mainErr := dm.mainDB.db.Get([]byte(key), nil)
		if mainErr != nil && mainErr != leveldb.ErrNotFound {
			return nil, mainErr
		}
		if len(mainValue) == 0 {
			log.Printf("Found %s on temp", key)
			return value, nil
		}

		// TODO: compare version of state to decide which one is the latest state
		return mainValue, mainErr
	}

	return dm.mainDB.db.Get([]byte(key), nil)
}
