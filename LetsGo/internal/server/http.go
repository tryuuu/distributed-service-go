package server

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
)

type httpServer struct {
	// log.goで定義された構造体
	Log *Log
}

func newHTTPServer() *httpServer {
	// log.goで定義されている
	return &httpServer{
		Log: NewLog(),
	}
}

type ProduceRequest struct {
	Record Record `json:"record"`
}

type ProduceResponse struct {
	Offset uint64 `json:"offset"`
}

type ConsumeRequest struct {
	Offset uint64 `json:"offset"`
}

type ConsumeResponse struct {
	Record Record `json:"record"`
}

// 引数はport番号
func NewHTTPServer(addr string) *http.Server {
	httpsrv := newHTTPServer()
	r := mux.NewRouter()
	r.HandleFunc("/", httpsrv.handleProduce).Methods("POST")
	r.HandleFunc("/", httpsrv.handleConsume).Methods("GET")
	return &http.Server{
		Addr:    addr,
		Handler: r,
	}
}

// curl -X POST localhost:8080 -d `{"record": {"value": "test"}}` といったリクエストを受け取る
func (s *httpServer) handleProduce(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	var req ProduceRequest
	// リクエストのボディをデコード(jsonからの変換)してreqに格納(ProduceRequest型)
	// 格納例: req = {Record: {Value: "value", Offset: 0}}
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	off, err := s.Log.Append(req.Record)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	res := ProduceResponse{Offset: off}
	// レスポンスをjsonにエンコードして返す
	// 例: {"offset": 0}を返す
	err = json.NewEncoder(w).Encode(res)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// curl -X GET localhost:8080 -d '{"offset": 2}'といったリクエストを受け取る
func (s *httpServer) handleConsume(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	var req ConsumeRequest
	// リクエストのクエリをjsonからデコードしてreqに格納(ConsumeRequest型)
	// 格納例: req = {Offset: 0}
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	rec, err := s.Log.Read(req.Offset)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	// Record構造体で定義されている形式
	res := ConsumeResponse{Record: rec}
	// レスポンスをエンコードして返す
	// 例: {"record": {"value": "test", "offset": 0}}
	err = json.NewEncoder(w).Encode(res)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
