package db

import (
	"log"
	"path"
)

type LevelDBManagerAddBackup struct {
	path               string
	mainDB             *LevelDBNormal
	backupDB           *LevelDBManager
	backupAsyncChannel chan message

	waitForBackup bool
}

type message struct {
	key   string
	value []byte
}

func NewLevelDBManagerAddBackup(dbPath string, waitForBackup bool) (*LevelDBManagerAddBackup, error) {
	mainPath := path.Join(dbPath, "live")
	backupPath := path.Join(dbPath, "backup")
	mainDB, err := NewLevelDBNormal(mainPath)
	if err != nil {
		return nil, err
	}

	backupDB, err := NewDB(backupPath)
	if err != nil {
		return nil, err
	}
	dbManager := &LevelDBManagerAddBackup{
		path:               dbPath,
		mainDB:             mainDB,
		backupDB:           backupDB,
		waitForBackup:      waitForBackup,
		backupAsyncChannel: make(chan message, 10000),
	}
	if !waitForBackup {
		go dbManager.startAsyncWriteBackup()
	}
	return dbManager, nil
}

func (dm *LevelDBManagerAddBackup) Put(key string, value []byte) error {
	if !dm.waitForBackup {
		dm.backupAsyncChannel <- message{
			key:   key,
			value: value,
		}
	} else {
		err := dm.backupDB.Put(key, value)
		if err != nil {
			return err
		}
	}
	return dm.mainDB.Put(key, value)
}

func (dm *LevelDBManagerAddBackup) Get(key string) ([]byte, error) {
	return dm.mainDB.Get(key)
}

func (dm *LevelDBManagerAddBackup) startAsyncWriteBackup() {
	for task := range dm.backupAsyncChannel {
		msg := task
		err := dm.backupDB.Put(msg.key, msg.value)
		if err != nil {
			log.Printf("[catch me] error when put in backup db %s: %s", msg.key, err.Error())
		}
	}
}
