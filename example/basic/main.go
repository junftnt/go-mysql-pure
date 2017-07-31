package main

import (
	"flag"
	"fmt"
	"os"

	//"github.com/davecgh/go-spew/spew"
	"github.com/junhsieh/go-mysql-pure"
)

func exit(err *error) {
	if *err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", *err)

		os.Exit(1)
	}
}

func main() {
	//
	err := new(error)

	defer func(err *error) {
		exit(err)
	}(err)

	//
	host := flag.String("host", "", "Host")
	port := flag.String("port", "3306", "Port")
	dbName := flag.String("dbName", "", "Database name")
	username := flag.String("username", "", "Username")
	password := flag.String("password", "", "Password")

	flag.Parse()

	//
	conn := mysql.NewConnection(mysql.ConnectionParameter{
		Network:       "tcp",
		Host:          *host,
		Port:          *port,
		DBName:        *dbName,
		Username:      *username,
		Password:      *password,
		IsDebugPacket: true,
	})

	//
	*err = conn.Open()

	if *err != nil {
		return
	}

	defer conn.Close()
}
