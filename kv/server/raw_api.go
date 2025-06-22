package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	r, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	val, err := r.GetCF(req.Cf, req.Key)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return &kvrpcpb.RawGetResponse{
			Value:    nil,
			NotFound: true,
		}, nil
	}

	rsp := &kvrpcpb.RawGetResponse{
		Value: val,
	}
	return rsp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	m := storage.Modify{
		Data: storage.Put{
			Key:   req.Key,
			Value: req.Value,
			Cf:    req.Cf,
		},
	}

	err := server.storage.Write(req.Context, []storage.Modify{m})
	if err != nil {
		return nil, err
	}

	rsp := &kvrpcpb.RawPutResponse{}
	return rsp, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	m := storage.Modify{
		Data: storage.Delete{
			Key: req.Key,
			Cf:  req.Cf,
		},
	}

	err := server.storage.Write(req.Context, []storage.Modify{m})
	if err != nil {
		return nil, err
	}

	rsp := &kvrpcpb.RawDeleteResponse{}
	return rsp, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	serverReader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer serverReader.Close()

	it := serverReader.IterCF(req.Cf)
	defer it.Close()

	kvPairs := make([]*kvrpcpb.KvPair, 0, req.Limit)
	cnt := 0

	for it.Seek(req.StartKey); it.Valid(); it.Next() {
		item := it.Item()
		key := item.KeyCopy(nil)

		value, err := item.Value()
		if err != nil {
			return nil, err
		}

		kvPairs = append(kvPairs, &kvrpcpb.KvPair{
			Key:   key,
			Value: value,
		})

		cnt++
		if cnt >= int(req.Limit) {
			break
		}
	}

	rsp := &kvrpcpb.RawScanResponse{
		Kvs: kvPairs,
	}
	return rsp, nil
}
