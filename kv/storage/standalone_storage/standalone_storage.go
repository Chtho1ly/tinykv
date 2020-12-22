package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"os"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	db     *badger.DB
	dbPath string
	config  *config.Config
}

type SAReader struct {
	txn *badger.Txn
}

func NewSAReader(kv *badger.DB) *SAReader {
	return &SAReader{
		txn: kv.NewTransaction(false),
	}
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	dbPath := conf.DBPath

	_ = os.MkdirAll(dbPath, os.ModePerm)

	standAloneDB := engine_util.CreateDB(dbPath, false)

	return &StandAloneStorage{db: standAloneDB,dbPath: dbPath, config: conf}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	//if s.db==nil{
	//	return
	//}
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	if err := s.db.Close(); err != nil {
		return err
	}
	return nil
}

func (r *SAReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(r.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return val, err
}

func (r *SAReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.txn)
}

func (r *SAReader) Close() {
	r.txn.Discard()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return NewSAReader(s.db), nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	for _, m := range batch {
		switch m.Data.(type) {
		case storage.Put:
			put := m.Data.(storage.Put)
			engine_util.PutCF(s.db,put.Cf,put.Key,put.Value)
		case storage.Delete:
			delete := m.Data.(storage.Delete)
			engine_util.DeleteCF(s.db,delete.Cf,delete.Key)
		}
	}
	return nil
}
