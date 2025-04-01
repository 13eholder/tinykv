package mvcc

import (
	"bytes"

	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	// Your Data Here (4C).
	txn  *MvccTxn
	iter engine_util.DBIterator
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	iter.Seek(EncodeKey(startKey, txn.StartTS))
	return &Scanner{
		txn:  txn,
		iter: iter,
	}
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	scan.iter.Close()
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	if !scan.iter.Valid() {
		return nil, nil, nil
	}
	item := scan.iter.Item()
	key := DecodeUserKey(item.Key())
	value, _ := item.Value()
	// 一个key对应多个操作,只取最新的一次,丢弃掉旧操作
	for ; scan.iter.Valid() && (bytes.Equal(DecodeUserKey(scan.iter.Item().Key()), key) || decodeTimestamp(scan.iter.Item().Key()) > scan.txn.StartTS); scan.iter.Next() {
	}
	write, _ := ParseWrite(value)

	if write.Kind == WriteKindDelete {
		return key, nil, nil
	}

	value, _ = scan.txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(key, write.StartTS))
	return key, value, nil
}
