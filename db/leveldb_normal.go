package db

import (
	"log"
	"sync"
)

type LevelDBNormal struct {
	db *levelDBWrapper
}

func NewLevelDBNormal(path string) (*LevelDBNormal, error) {
	log.Printf("Create newDB at path: %s", path)
	db := &levelDBWrapper{
		path: path,
		wg:   &sync.WaitGroup{},
	}

	if err := db.open(); err != nil {
		return nil, err
	}

	return &LevelDBNormal{db: db}, nil
}

func (dm *LevelDBNormal) Put(key string, value []byte) error {
	return dm.db.put([]byte(key), value)
}

func (dm *LevelDBNormal) Get(key string) ([]byte, error) {
	return dm.db.db.Get([]byte(key), nil)
}
