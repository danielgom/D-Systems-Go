package main

import (
	"github.com/danielgom/proglog/internal/server"
	"log"
)

func main() {

	srv := server.NewHttpServer(":8080")
	log.Fatal(srv.ListenAndServe())

}
