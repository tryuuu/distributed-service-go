package server

import (
	"fmt"
	"sync"
)

type Log struct {
	mu      sync.Mutex
	records []Record
}

// json時には“内の変数を使用
type Record struct {
	Value  []byte `json:"value"`
	Offset uint64 `json:"offset"`
}

var ErrOffsetNotFound = fmt.Errorf("offset not found")

func NewLog() *Log {
	return &Log{}
}

// リストに追加しリストの長さを返す
func (c *Log) Append(record Record) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	record.Offset = uint64(len(c.records))
	c.records = append(c.records, record)
	return record.Offset, nil
}

// 引数で受け取ったoffsetの位置のRecordを返す
func (c *Log) Read(offset uint64) (Record, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if offset >= uint64(len(c.records)) {
		return Record{}, ErrOffsetNotFound
	}
	return c.records[offset], nil
}
