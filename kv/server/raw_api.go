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
	// Your Code Here (1).
	r, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}
	value, err := r.GetCF(req.GetCf(), req.GetKey())
	if err != nil {
		return nil, err
	}
	resp := &kvrpcpb.RawGetResponse{
		Value:    value,
		NotFound: value == nil,
	}
	r.Close()
	return resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	data := storage.Put{
		Key:   req.GetKey(),
		Value: req.GetValue(),
		Cf:    req.GetCf(),
	}
	batch := []storage.Modify{{Data: data}}
	err := server.storage.Write(nil, batch)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	data := storage.Delete{Key: req.GetKey(), Cf: req.GetCf()}
	batch := []storage.Modify{{Data: data}}
	err := server.storage.Write(nil, batch)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	kvs := make([]*kvrpcpb.KvPair, 0)
	r, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}
	iter := r.IterCF(req.GetCf())
	iter.Seek(req.GetStartKey())
	num := 0
	for iter.Valid() && num < int(req.Limit) {
		item := iter.Item()
		value, _ := item.Value()
		kvs = append(kvs, &kvrpcpb.KvPair{Key: item.Key(), Value: value})
		iter.Next()
		num++
	}
	iter.Close()
	r.Close()
	resp := &kvrpcpb.RawScanResponse{Kvs: kvs}
	return resp, nil
}
