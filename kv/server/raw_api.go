package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"

	"github.com/pingcap-incubator/tinykv/kv/storage"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse,error) {
	// Your Code Here (1).
	var response kvrpcpb.RawGetResponse
	re,err:=server.storage.Reader(req.Context)
	if  err != nil{
		return nil,err
	}
	defer re.Close()
	response.Value,err=re.GetCF(req.Cf,req.Key)
	if  err != nil{
		return nil,err
	}
	if response.Value == nil{
		response.NotFound=true
	}	
	return &response, err
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	var response kvrpcpb.RawPutResponse
	batch := []storage.Modify{
		{
			Data: storage.Put{
				Cf:    req.Cf,
				Key:   req.Key,
				Value: req.Value,
			},
		}}
	err := server.storage.Write(req.Context,batch)
	if err != nil{
		return nil,err
	}
	return &response, err
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	var response kvrpcpb.RawDeleteResponse
	batch := []storage.Modify{
		{
			Data: storage.Delete{
				Cf:  req.Cf,
				Key: req.Key,
			},
		}}
	err := server.storage.Write(req.Context,batch)
	if err != nil{
		return nil,err
	}
	return &response, err
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	var response kvrpcpb.RawScanResponse
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	itor := reader.IterCF(req.Cf)
	defer itor.Close()
	i := req.Limit
	var kvs []*kvrpcpb.KvPair
	for itor.Seek(req.StartKey); itor.Valid(); itor.Next(){
		if(i==0){
			break
		}
		item := itor.Item()
		val, err := item.ValueCopy(nil)
		if err != nil{
			return nil,err
		}
		kvs = append(kvs, &kvrpcpb.KvPair{
		Key:   item.KeyCopy(nil),
		Value: val,			
		})
		i--
	}
	response.Kvs =kvs
	return &response, err
}
