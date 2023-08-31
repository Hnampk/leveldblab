package db

import (
	"fmt"
	"leveldblab/config"
	"log"
	"path"
	"sync"
	"time"

	cp "github.com/otiai10/copy"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

var (
	wo = &opt.WriteOptions{
		NoWriteMerge: false,
		Sync:         false,
	}

	requestKey      = "REQ"
	batchRequestKey = "BATCHREQ"
	dbRootFolder    = ""
	dbFilePath      = ""
)

type DBRepo struct {
	mainDB *leveldb.DB
	tempDB *leveldb.DB

	onMerging bool
	onBackUp  bool

	sync.Mutex
}

func NewDBRepository(rootFolder, dbFolder string) *DBRepo {
	dbRootFolder = rootFolder
	dbFilePath = path.Join(dbRootFolder, dbFolder)
	log.Printf("NewDBRepository db path: %s", dbFilePath)

	dbRepo := &DBRepo{}
	dbRepo.openMainDB()
	dbRepo.openTempDB()
	dbRepo.mergeTempDB()

	if config.EnableBackup {
		go func() {
			for {
				ticker := time.NewTicker(30 * time.Second)
				<-ticker.C

				if err := dbRepo.backupMainDB(); err != nil {
					log.Printf("error while backupMainDB: %s", err.Error())
					continue
				}

				// backup done => merge data
				dbRepo.onMerging = true
				hasError := dbRepo.mergeTempDB()
				if !hasError {
					dbRepo.onMerging = false
				}
			}
		}()
	}

	return dbRepo
}

func (p *DBRepo) openMainDB() error {
	if p.mainDB != nil {
		return nil
	}

	dbFile, err := leveldb.OpenFile(dbFilePath, nil)
	if err != nil {
		log.Fatalf("Error load db folder %s: %v", dbFilePath, err)
		return err
	}
	p.mainDB = dbFile
	return nil
}

func (p *DBRepo) closeMainDB() error {
	if err := p.mainDB.Close(); err != nil {
		return err
	}
	p.mainDB = nil

	return nil
}

func (p *DBRepo) openTempDB() {
	if p.tempDB != nil {
		return
	}

	dbFile, err := leveldb.OpenFile(dbRootFolder+"/temp", nil)
	if err != nil {
		log.Fatalf("Error load db folder %s: %s", dbFilePath, err.Error())
	}
	p.tempDB = dbFile
}

// func (p *DBRepo) closeTempDB() error {
// 	if p.tempDB != nil {
// 		return nil
// 	}

// 	if err := p.tempDB.Close(); err != nil {
// 		return err
// 	}

// 	p.tempDB = nil
// 	return nil
// }

func (p *DBRepo) backupMainDB() error {
	start := time.Now()
	defer func(start time.Time) {
		log.Printf("Backup done after %dms\n", time.Since(start).Milliseconds())
	}(start)

	p.Lock()
	defer p.Unlock()

	p.onBackUp = true
	backupName := fmt.Sprintf("./backup-%d/%s", time.Now().Nanosecond(), dbFilePath)

	log.Println("Start backup, sleep for 1s before close DB", backupName)
	time.Sleep(1 * time.Second)
	if err := p.closeMainDB(); err != nil {
		log.Printf("error while closeMainDB: %s", err.Error())
		return err
	}
	// if err := p.mainDB.SetReadOnly(); err != nil {
	// 	log.Printf("error while SetReadOnly: %s", err.Error())
	// 	return err
	// }
	if err := cp.Copy(dbFilePath, backupName); err != nil {
		log.Printf("error while Copy: %s", err.Error())
		return err
	}
	if err := p.openMainDB(); err != nil {
		log.Printf("error while openMainDB: %s", err.Error())
		return err
	}

	p.onBackUp = false
	return nil
}

func (p *DBRepo) mergeTempDB() bool {
	start := time.Now()
	iter := p.tempDB.NewIterator(util.BytesPrefix([]byte("")), nil)
	defer iter.Release()
	hasError := false
	count := 0
	for iter.Next() {
		count++
		key := iter.Key()
		value := iter.Value()

		// TODO: check on state version before overwrite data
		if err := p.mainDB.Put(key, value, wo); err != nil {
			hasError = true
			continue
		} else {
			if err := p.tempDB.Delete(key, wo); err != nil {
				continue
			}
		}
	}

	log.Printf("Merged tempDB %d records. Status: %t. Duration: %dms", count, !hasError, time.Since(start).Milliseconds())
	return hasError
}

// Get find a key in DB
func (p *DBRepo) Get(key string) ([]byte, error) {
	value, err := p.tempDB.Get([]byte(key), nil)
	if err != nil && err != leveldb.ErrNotFound {
		return nil, err
	}

	if p.onBackUp {
		p.Lock()
		defer p.Unlock()
	}
	if len(value) > 0 {
		mainValue, err := p.mainDB.Get([]byte(key), nil)
		if err != nil && err != leveldb.ErrNotFound {
			return nil, err
		}
		if len(mainValue) == 0 {
			log.Printf("Found %s on temp", key)
			return value, nil
		}

		// TODO: compare version of state to decide which one is the latest state
		return mainValue, err
	}

	return p.mainDB.Get([]byte(key), nil)
}

// Put save a value into db
func (p *DBRepo) Put(key string, value []byte) error {
	if p.onBackUp {
		return p.tempDB.Put([]byte(key), value, wo)
	}

	return p.mainDB.Put([]byte(key), value, wo)
}

// Delete a value from db
func (p *DBRepo) Del(key string) error {
	if p.onMerging {
		if err := p.tempDB.Delete([]byte(key), wo); err != nil {
			return err
		}
		if err := p.mainDB.Delete([]byte(key), wo); err != nil {
			return err
		}

		return nil
	}

	if p.onBackUp {
		return p.tempDB.Delete([]byte(key), wo)
	}
	return p.mainDB.Delete([]byte(key), wo)
}

// Iterator get an iterator of a key
func (p *DBRepo) Iterator(key string) []iterator.Iterator {
	for p.onMerging {
		time.Sleep(50 * time.Millisecond)
	}

	if p.onBackUp {
		return []iterator.Iterator{
			p.tempDB.NewIterator(util.BytesPrefix([]byte(key)), nil),
			p.mainDB.NewIterator(util.BytesPrefix([]byte(key)), nil),
		}
	}

	return []iterator.Iterator{
		p.mainDB.NewIterator(util.BytesPrefix([]byte(key)), nil),
	}
}
