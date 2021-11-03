package main

import (
	"fmt"
	"os"
	"time"

	"github.com/brave/go-sync-adm-tools/dynamo"
)

const (
	ttl = time.Hour // 1 hour
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("unsupported commands")
		os.Exit(1)
	}

	db, err := dynamo.NewDynamo()
	if err != nil {
		fmt.Printf("Initialize dynamoDB session failed: %s\n", err.Error())
	}
	switch os.Args[1] {
	case "delete":
		if len(os.Args) < 3 {
			fmt.Println("missing ClientID arg")
			os.Exit(1)
		}
		err := db.DeleteUserData(os.Args[2], time.Now().Add(ttl).Unix())
		if err != nil {
			fmt.Println(err.Error())
		}
	default:
		fmt.Println("unsupported commands")
		os.Exit(1)
	}
}
