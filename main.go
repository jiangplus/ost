package main

import (
	"flag"
	"fmt"
	//"github.com/aws/aws-sdk-go/aws"
	//"github.com/aws/aws-sdk-go/aws/session"
	//"github.com/aws/aws-sdk-go/service/s3"
	//"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"os"
)

func main() {
	lsCmd := flag.NewFlagSet("ls", flag.ExitOnError)
	getCmd := flag.NewFlagSet("get", flag.ExitOnError)
	putCmd := flag.NewFlagSet("put", flag.ExitOnError)
	delCmd := flag.NewFlagSet("del", flag.ExitOnError)
	infoCmd := flag.NewFlagSet("info", flag.ExitOnError)
	cpCmd := flag.NewFlagSet("cp", flag.ExitOnError)
	modifyCmd := flag.NewFlagSet("modify", flag.ExitOnError)
	setaclCmd := flag.NewFlagSet("setacl", flag.ExitOnError)
	syncCmd := flag.NewFlagSet("sync", flag.ExitOnError)
	multipartCmd := flag.NewFlagSet("multipart", flag.ExitOnError)
	abortmpCmd := flag.NewFlagSet("abortmp", flag.ExitOnError)
	listmpCmd := flag.NewFlagSet("listmp", flag.ExitOnError)

	switch os.Args[1] {
	case "ls":
		lsCmd.Parse(os.Args[2:])
	case "get":
		getCmd.Parse(os.Args[2:])
	case "put":
		putCmd.Parse(os.Args[2:])
	case "del":
		delCmd.Parse(os.Args[2:])
	case "info":
		infoCmd.Parse(os.Args[2:])
	case "cp":
		cpCmd.Parse(os.Args[2:])
	case "modify":
		modifyCmd.Parse(os.Args[2:])
	case "setacl":
		setaclCmd.Parse(os.Args[2:])
	case "sync":
		syncCmd.Parse(os.Args[2:])
	case "multipart":
		multipartCmd.Parse(os.Args[2:])
	case "abortmp":
		abortmpCmd.Parse(os.Args[2:])
	case "listmp":
		listmpCmd.Parse(os.Args[2:])
	default:
		fmt.Println("expected subcommands to run")
		os.Exit(1)
	}
}