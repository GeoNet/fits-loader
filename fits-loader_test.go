package main

import (
	"log"
)

// setup starts a db connection and test server then inits an http client.
func setup() {
	config.DataBase.SSLMode = "disable"
	if err := config.initDB(); err != nil {
		log.Fatal(err)
	}
}

// teardown closes the db connection and  test server.  Defer this after setup() e.g.,
// ...
// setup()
// defer teardown()
func teardown() {
	db.Close()
}
