package main

import (
	"log"

	"github.com/tryu/proglog/internal/server" // go mod init github.com/tryu/proglogをしているため
)

func main() {
	srv := server.NewHTTPServer(":8080")
	log.Fatal(srv.ListenAndServe())
}
