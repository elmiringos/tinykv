package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	Engine *engine_util.Engines
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	db := engine_util.CreateDB(conf.DBPath, false)

	engine := engine_util.NewEngines(db, nil, conf.DBPath, "")

	storage := &StandAloneStorage{Engine: engine}

	return storage
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return nil
}

type standaloneStorageReader struct {
	txn *badger.Txn
}

func (s *StandAloneStorage) newStandaloneStorageReader() *standaloneStorageReader {
	txn := s.Engine.Kv.NewTransaction(false)

	return &standaloneStorageReader{
		txn: txn,
	}
}

func (r *standaloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(r.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return val, nil
}

func (r *standaloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.txn)
}

func (r *standaloneStorageReader) Close() {
	r.txn.Discard()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	return s.newStandaloneStorageReader(), nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	wb := new(engine_util.WriteBatch)

	for _, m := range batch {
		switch m.Data.(type) {
		case storage.Put:
			wb.SetCF(m.Cf(), m.Key(), m.Value())
		case storage.Delete:
			wb.DeleteCF(m.Cf(), m.Key())
		}
	}

	return wb.WriteToDB(s.Engine.Kv)
}
