package server

import (
	"context"
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	reader, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	val, err := reader.GetCF(req.GetCf(), req.GetKey())
	var resp *kvrpcpb.RawGetResponse
	if val == nil {
		resp = &kvrpcpb.RawGetResponse{
			RegionError:          nil,
			Error:                badger.ErrKeyNotFound.Error(),
			Value:                nil,
			NotFound:             true,
			XXX_NoUnkeyedLiteral: struct{}{},
			XXX_unrecognized:     nil,
			XXX_sizecache:        0,
		}
	} else {
		resp = &kvrpcpb.RawGetResponse{
			RegionError:          nil,
			Error:                "",
			Value:                val,
			NotFound:             false,
			XXX_NoUnkeyedLiteral: struct{}{},
			XXX_unrecognized:     nil,
			XXX_sizecache:        0,
		}
	}

	return resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	err := server.storage.Write(nil, []storage.Modify{
		{
			Data: storage.Put{
				Key:   req.GetKey(),
				Value: req.GetValue(),
				Cf:    req.GetCf(),
			},
		}})
	if err != nil {
		return nil, err
	}

	return &kvrpcpb.RawPutResponse{
		RegionError:          nil,
		Error:                "",
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_unrecognized:     nil,
		XXX_sizecache:        0,
	}, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	err := server.storage.Write(nil, []storage.Modify{
		{
			Data: storage.Delete{
				Key: req.GetKey(),
				Cf:  req.GetCf(),
			},
		}})
	if err != nil {
		return nil, err
	}

	return &kvrpcpb.RawDeleteResponse{
		RegionError:          nil,
		Error:                "",
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_unrecognized:     nil,
		XXX_sizecache:        0,
	}, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	kvs := make([]*kvrpcpb.KvPair, 0)
	iterCF := reader.IterCF(req.GetCf())
	defer iterCF.Close()
	iterCF.Seek(req.GetStartKey())

	for iterCF.Valid() && uint32(len(kvs)) < req.Limit {
		item := iterCF.Item()
		value, _ := item.Value()
		key := item.Key()
		kvs = append(kvs, &kvrpcpb.KvPair{
			Error:                nil,
			Key:                  key,
			Value:                value,
			XXX_NoUnkeyedLiteral: struct{}{},
			XXX_unrecognized:     nil,
			XXX_sizecache:        0,
		})
		iterCF.Next()
	}

	return &kvrpcpb.RawScanResponse{
		RegionError:          nil,
		Error:                "",
		Kvs:                  kvs,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_unrecognized:     nil,
		XXX_sizecache:        0,
	}, nil
}
