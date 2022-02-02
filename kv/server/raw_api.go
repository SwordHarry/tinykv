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
	reader, err := server.storage.Reader(req.GetContext())
	resp := new(kvrpcpb.RawGetResponse)
	if err != nil {
		resp.Error = err.Error()
		return resp, err
	}
	defer reader.Close()
	val, err := reader.GetCF(req.GetCf(), req.GetKey())
	if err != nil {
		resp.Error = err.Error()
		return resp, err
	}
	// key not found
	if val == nil {
		resp.NotFound = true
	}
	resp.Value = val
	return resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	put := storage.Put{
		Key:   req.GetKey(),
		Value: req.GetValue(),
		Cf:    req.GetCf(),
	}
	modify := storage.Modify{
		Data: put,
	}
	err := server.storage.Write(req.GetContext(), []storage.Modify{modify})
	resp := new(kvrpcpb.RawPutResponse)
	if err != nil {
		resp.Error = err.Error()
		return resp, err
	}
	return resp, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	del := storage.Delete{
		Key: req.GetKey(),
		Cf:  req.GetCf(),
	}
	modify := storage.Modify{Data: del}
	err := server.storage.Write(req.GetContext(), []storage.Modify{modify})
	resp := new(kvrpcpb.RawDeleteResponse)
	if err != nil && err != badger.ErrKeyNotFound {
		resp.Error = err.Error()
		return resp, err
	}
	return resp, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	resp := new(kvrpcpb.RawScanResponse)
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		resp.Error = err.Error()
		return resp, nil
	}
	defer reader.Close()
	iter := reader.IterCF(req.GetCf())
	defer iter.Close()
	var kvs []*kvrpcpb.KvPair
	// limit is the max read item num
	limit := req.GetLimit()
	// startKey, the key prefix
	startKey := req.GetStartKey()
	for iter.Seek(startKey); iter.Valid() && len(kvs) < int(limit); iter.Next() {
		item := iter.Item()
		key := item.Key()
		val, _ := item.Value()
		kv := &kvrpcpb.KvPair{
			Key:   key,
			Value: val,
		}
		kvs = append(kvs, kv)
	}
	resp.Kvs = kvs
	return resp, nil
}
