package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var (
	enc = binary.BigEndian
)

const (
	lenWidth = 8
)

type store struct {
	*os.File
	mu   sync.Mutex
	buf  *bufio.Writer
	size uint64
}

func NewStore(f *os.File) (*store, error) {
	// ファイルの現在の情報を取得
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	// ファイルのサイズを取得
	size := uint64(fi.Size())
	return &store{
		File: f,
		size: size,
		buf:  bufio.NewWriter(f),
	}, nil
}

// 説明はappend_explanation.png
func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pos = s.size
	// バイト列の長さを書き込む
	if err := binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		return 0, 0, err
	}
	// バイト列を書き込む
	w, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}
	// wには「長さ(8byte) + データ本体」の合計バイト数が格納される
	w += lenWidth
	// ストアの現在のファイルサイズを更新
	s.size += uint64(w)
	// uint64(w)は書き込まれた合計バイト数、posは書き込み開始位置（＝Append 前の s.size）
	return uint64(w), pos, nil
}

// 指定された位置に格納されているバイト列を読み出す
func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}
	// lenWidthバイトの長さを持つバッファを作成
	size := make([]byte, lenWidth)

	// posからlen(size)分読み込み、読み込んだbyte数を返す
	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}
	// encでsizeバッファに格納されたデータの長さを取得
	b := make([]byte, enc.Uint64(size))
	// それを用いて実際のデータを読み出す(pos+lenWidthは実際のデータの開始位置)
	if _, err := s.File.ReadAt(b, int64(pos+lenWidth)); err != nil {
		return nil, err
	}
	return b, nil
}

func (s *store) ReadAt(p []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return 0, err
	}
	return s.File.ReadAt(p, off)
}

func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.buf.Flush()
	if err != nil {
		return err
	}
	return s.File.Close()
}
