package server

import (
	"context"
	"log"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	keysToLatch := [][]byte{req.Key}
	server.Latches.WaitForLatches(keysToLatch)
	defer server.Latches.ReleaseLatches(keysToLatch)

	resp := new(kvrpcpb.GetResponse)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err
	}
	mvccTxn := mvcc.NewMvccTxn(reader, req.Version)
	lock, err := mvccTxn.GetLock(req.Key)
	if err != nil {
		return resp, err
	}
	if lock != nil && lock.IsLockedFor(req.Key, req.Version, resp) {
		return resp, nil
	}

	value, err := mvccTxn.GetValue(req.Key)
	if err != nil {
		return resp, err
	}
	if value == nil {
		resp.NotFound = true
	}
	resp.Value = value
	return resp, nil
}

type LockResponse struct {
	Error *kvrpcpb.KeyError
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	var keysToLatch [][]byte
	for _, mutation := range req.Mutations {
		keysToLatch = append(keysToLatch, mutation.Key)
	}
	server.Latches.WaitForLatches(keysToLatch)
	defer server.Latches.ReleaseLatches(keysToLatch)

	resp := new(kvrpcpb.PrewriteResponse)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err
	}
	mvccTxn := mvcc.NewMvccTxn(reader, req.StartVersion)

	for _, key := range keysToLatch {
		write, commmitTs, err := mvccTxn.MostRecentWrite(key)
		if err != nil {
			return resp, err
		}
		if write != nil && commmitTs > req.StartVersion {
			resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{
				Conflict: &kvrpcpb.WriteConflict{
					StartTs:    write.StartTS,
					ConflictTs: commmitTs,
					Key:        key,
					Primary:    req.PrimaryLock,
				},
			})
		}

		lock, err := mvccTxn.GetLock(key)
		if err != nil {
			return resp, err
		}
		lockResp := &LockResponse{}
		if lock != nil && lock.IsLockedFor(key, req.StartVersion, lockResp) {
			if lockResp.Error != nil {
				resp.Errors = append(resp.Errors, lockResp.Error)
			}
		}
	}

	if len(resp.Errors) > 0 {
		return resp, nil
	}

	for _, mutation := range req.Mutations {
		lock := &mvcc.Lock{
			Primary: req.PrimaryLock,
			Ts:      req.StartVersion,
			Ttl:     req.LockTtl,
		}
		switch mutation.Op {
		case kvrpcpb.Op_Put:
			mvccTxn.PutValue(mutation.Key, mutation.Value)
			lock.Kind = mvcc.WriteKindPut
		case kvrpcpb.Op_Del:
			mvccTxn.DeleteValue(mutation.Key)
			lock.Kind = mvcc.WriteKindDelete
		default:
			log.Panic("unsupported type")
		}
		mvccTxn.PutLock(mutation.Key, lock)
	}
	err = server.storage.Write(req.Context, mvccTxn.Writes())
	if err != nil {
		return resp, err
	}
	return resp, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)

	resp := new(kvrpcpb.CommitResponse)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	mvccTxn := mvcc.NewMvccTxn(reader, req.StartVersion)
	for _, key := range req.Keys {
		lock, _ := mvccTxn.GetLock(key)
		if lock == nil {
			// 回滚操作
			write, _, err := mvccTxn.CurrentWrite(key)
			if err != nil {
				return resp, err
			}
			if write != nil && write.Kind == mvcc.WriteKindRollback {
				resp.Error = &kvrpcpb.KeyError{
					Retryable: "true",
				}
				return resp, nil
			}
		} else if lock.Ts < req.StartVersion {
			resp.Error = &kvrpcpb.KeyError{
				Retryable: "true",
			}
			return resp, nil
		}

	}
	for _, key := range req.Keys {
		lock, _ := mvccTxn.GetLock(key)
		if lock == nil {
			// 重复提交的请求,忽略
			continue
		}
		mvccTxn.PutWrite(key, req.CommitVersion, &mvcc.Write{
			StartTS: req.StartVersion,
			Kind:    lock.Kind,
		})
		mvccTxn.DeleteLock(key)
	}
	err = server.storage.Write(req.Context, mvccTxn.Writes())
	if err != nil {
		return resp, err
	}
	return resp, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	resp := new(kvrpcpb.ScanResponse)

	reader, _ := server.storage.Reader(req.Context)
	mvccTxn := mvcc.NewMvccTxn(reader, req.Version)
	scanner := mvcc.NewScanner(req.StartKey, mvccTxn)
	defer scanner.Close()

	kvPairs := make([]*kvrpcpb.KvPair, 0)
	for i := 0; i < int(req.Limit); i++ {
		key, value, _ := scanner.Next()
		if key == nil && value == nil {
			break
		}
		kv := &kvrpcpb.KvPair{}
		lock, _ := mvccTxn.GetLock(key)
		if lock != nil && lock.IsLockedFor(key, req.Version, kv) {
			kvPairs = append(kvPairs, kv)
			continue
		}
		if value != nil {
			kv.Key = key
			kv.Value = value
			kvPairs = append(kvPairs, kv)
		}
	}
	resp.Pairs = kvPairs
	return resp, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	resp := new(kvrpcpb.CheckTxnStatusResponse)

	reader, _ := server.storage.Reader(req.Context)
	mvccTxn := mvcc.NewMvccTxn(reader, req.LockTs)

	// 检测是否提交,提交的话就忽略
	write, commitTs, _ := mvccTxn.CurrentWrite(req.PrimaryKey)
	if write != nil && write.Kind != mvcc.WriteKindRollback {
		resp.CommitVersion = commitTs
		return resp, nil
	}

	lock, _ := mvccTxn.GetLock(req.PrimaryKey)

	if lock == nil {
		if write != nil && write.Kind == mvcc.WriteKindRollback {
			resp.Action = kvrpcpb.Action_NoAction
			return resp, nil
		}
		mvccTxn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{
			StartTS: req.LockTs,
			Kind:    mvcc.WriteKindRollback,
		})
		server.storage.Write(req.Context, mvccTxn.Writes())
		resp.Action = kvrpcpb.Action_LockNotExistRollback
		return resp, nil
	}
	curTime := mvcc.PhysicalTime(req.CurrentTs)
	lockTime := mvcc.PhysicalTime(lock.Ts)

	if curTime >= lockTime && curTime-lockTime >= lock.Ttl {
		mvccTxn.DeleteLock(req.PrimaryKey)
		mvccTxn.DeleteValue(req.PrimaryKey)
		mvccTxn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{
			StartTS: req.LockTs,
			Kind:    mvcc.WriteKindRollback,
		})
		resp.Action = kvrpcpb.Action_TTLExpireRollback
	} else {
		resp.Action = kvrpcpb.Action_NoAction
	}
	server.storage.Write(req.Context, mvccTxn.Writes())
	return resp, nil
}

// 检查密钥是否被当前事务锁定，如果是，则删除锁定，删除值并将回滚指示器保留为写入
func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)

	resp := new(kvrpcpb.BatchRollbackResponse)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err
	}
	mvccTxn := mvcc.NewMvccTxn(reader, req.StartVersion)
	for _, key := range req.Keys {
		lock, _ := mvccTxn.GetLock(key)
		if lock != nil && lock.Ts == req.StartVersion {
			mvccTxn.DeleteLock(key)
			mvccTxn.DeleteValue(key)
			mvccTxn.PutWrite(key, req.StartVersion, &mvcc.Write{
				StartTS: req.StartVersion,
				Kind:    mvcc.WriteKindRollback,
			})
		} else {
			// 如果已被删除,并且没有回滚指示器,则添加
			write, _, _ := mvccTxn.CurrentWrite(key)
			if write == nil {
				mvccTxn.PutWrite(key, req.StartVersion, &mvcc.Write{
					StartTS: req.StartVersion,
					Kind:    mvcc.WriteKindRollback,
				})
			} else if write.StartTS == req.StartVersion && write.Kind != mvcc.WriteKindRollback {
				resp.Error = &kvrpcpb.KeyError{
					Abort: "true",
				}
			}
		}
	}
	err = server.storage.Write(req.Context, mvccTxn.Writes())
	if err != nil {
		return resp, err
	}
	return resp, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	resp := new(kvrpcpb.ResolveLockResponse)

	reader, _ := server.storage.Reader(req.Context)
	mvccTxn := mvcc.NewMvccTxn(reader, req.StartVersion)

	var keys [][]byte
	iter := mvccTxn.Reader.IterCF(engine_util.CfLock)
	defer iter.Close()
	for ; iter.Valid(); iter.Next() {
		item := iter.Item()
		value, _ := item.Value()
		lock, _ := mvcc.ParseLock(value)
		if lock.Ts == req.StartVersion {
			keys = append(keys, item.Key())
		}
	}

	if req.CommitVersion == 0 {
		// 全部回滚
		rb := &kvrpcpb.BatchRollbackRequest{
			Context:      req.Context,
			StartVersion: req.StartVersion,
			Keys:         keys,
		}
		rbResp, _ := server.KvBatchRollback(context.TODO(), rb)
		resp.Error = rbResp.Error
		resp.RegionError = rbResp.RegionError
	} else {
		commit := &kvrpcpb.CommitRequest{
			Context:       req.Context,
			StartVersion:  req.StartVersion,
			CommitVersion: req.CommitVersion,
			Keys:          keys,
		}
		commitResp, _ := server.KvCommit(context.TODO(), commit)
		resp.Error = commitResp.Error
		resp.RegionError = commitResp.RegionError
	}

	return resp, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
