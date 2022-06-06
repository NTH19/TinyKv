package standalone_storage

import (
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"

	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/Connor1996/badger"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	db *badger.DB
}
type Reader struct{
	txn *badger.Txn
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	d:=engine_util.CreateDB(conf.DBPath,conf.Raft)

	return &StandAloneStorage{
		db:d,
	}
	
}
func (reader *Reader)GetCF(cf string, key []byte) ([]byte,error) {
	val, err:=engine_util.GetCFFromTxn(reader.txn,cf,key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return val,err
}
func (reader *Reader)IterCF(cf string) engine_util.DBIterator{
	
	return engine_util.NewCFIterator(cf, reader.txn)
}
func (reader *Reader) Close(){
	reader.txn.Discard()
}
func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.db.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	txn:=s.db.NewTransaction(false)
	return &Reader{
		txn,
	}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	var wb engine_util.WriteBatch
	switch batch[0].Data.(type) {
	case storage.Put:
		for _,x := range batch{
			wb.SetCF(x.Cf(),x.Key(),x.Value())
		}
	case storage.Delete:
		for _,x :=range batch{
			wb.DeleteCF(x.Cf(),x.Key())
		}
	}
	return wb.WriteToDB(s.db)
}
