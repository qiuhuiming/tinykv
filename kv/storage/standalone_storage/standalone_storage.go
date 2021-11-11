package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pkg/errors"
	"path/filepath"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	db   *badger.DB
	conf *config.Config
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	kvPath := filepath.Join(conf.DBPath, "standalone")
	db := engine_util.CreateDB(kvPath, false)
	return &StandAloneStorage{
		db:   db,
		conf: conf,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	// Do nothing.
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.db.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	tx := s.db.NewTransaction(false)
	if tx == nil {
		return nil, errors.New("cannot start transaction")
	}
	return NewStandAloneReader(tx), nil
}

type StandAloneReader struct {
	txn *badger.Txn
}

func NewStandAloneReader(txn *badger.Txn) *StandAloneReader {
	return &StandAloneReader{txn: txn}
}

func (s *StandAloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	return engine_util.GetCFFromTxn(s.txn, cf, key)
}

func (s *StandAloneReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, s.txn)
}

func (s *StandAloneReader) Close() {
	s.txn.Discard()
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	txn := s.db.NewTransaction(true)
	if txn == nil {
		return errors.New("cannot start transaction")
	}
	defer txn.Discard()

	var err error
	for _, m := range batch {
		switch m.Data.(type) {
		case storage.Put:
			p := m.Data.(storage.Put)
			err = txn.Set(engine_util.KeyWithCF(p.Cf, p.Key), p.Value)
			if err != nil {
				break
			}
		case storage.Delete:
			d := m.Data.(storage.Delete)
			err = txn.Delete(engine_util.KeyWithCF(d.Cf, d.Key))
			if err != nil {
				break
			}
		}
	}
	if err != nil {
		return err
	}

	return txn.Commit()
}
