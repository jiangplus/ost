package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
)

func getPath(path string) string {
	return strings.TrimPrefix(path, "/")
}

func unmarkEtag(etag string) string {
	return strings.ReplaceAll(etag, "\"", "")
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("expected subcommands to run")
		os.Exit(0)
	}

	lsCmd := flag.NewFlagSet("ls", flag.ExitOnError)
	getCmd := flag.NewFlagSet("get", flag.ExitOnError)
	putCmd := flag.NewFlagSet("put", flag.ExitOnError)
	rmCmd := flag.NewFlagSet("del", flag.ExitOnError)
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
		if len(lsCmd.Args()) == 0 {
			ListBuckets()
		} else {
			s3path := lsCmd.Arg(0)
			ListObjects(s3path)
		}
	case "get":
		getCmd.Parse(os.Args[2:])
		if len(getCmd.Args()) != 2 {
			log.Fatal("source and dest is required")
		} else {
			localPath := getCmd.Arg(1)
			s3path := getCmd.Arg(0)
			GetObject(s3path, localPath)
		}
	case "put":
		putCmd.Parse(os.Args[2:])
		if len(putCmd.Args()) != 2 {
			log.Fatal("source and dest is required")
		} else {
			localPath := putCmd.Arg(0)
			s3path := putCmd.Arg(1)
			PutObject(s3path, localPath)
		}
	case "rm":
		rmCmd.Parse(os.Args[2:])
		if len(rmCmd.Args()) == 0 {
			log.Fatal("s3 uri is required")
		} else {
			s3path := rmCmd.Arg(0)
			RemoveObject(s3path)
		}
	case "info":
		infoCmd.Parse(os.Args[2:])
		if len(infoCmd.Args()) == 0 {
			log.Fatal("s3 uri is required")
		} else {
			s3path := infoCmd.Arg(0)
			ObjectInfo(s3path)
		}
	case "cp":
		cpCmd.Parse(os.Args[2:])
		if len(cpCmd.Args()) != 2 {
			log.Fatal("source and dest is required")
		} else {
			src := cpCmd.Arg(0)
			dst := cpCmd.Arg(1)
			CopyObject(src, dst)
		}
	case "modify":
		modifyCmd.Parse(os.Args[2:])
	case "setacl":
		setPublic := setaclCmd.Bool("acl-public", false, "Store objects with ACL allowing read for anyone.")
		setPrivate := setaclCmd.Bool("acl-private", false, "Store objects with default ACL allowing access for you only.")
		setaclCmd.Parse(os.Args[2:])

		if len(setaclCmd.Args()) == 0 {
			log.Fatal("s3 uri is required")
		} else {
			s3path := setaclCmd.Arg(0)
			SetaclObject(s3path, setPublic, setPrivate)
		}
	case "sync":
		syncCmd.Parse(os.Args[2:])
		if len(syncCmd.Args()) != 2 {
			log.Fatal("source and dest is required")
		} else {
			srcPath := syncCmd.Arg(0)
			dstPath := syncCmd.Arg(1)
			SyncDir(srcPath, dstPath)
		}
	case "multipart":
		multipartCmd.Parse(os.Args[2:])
		if len(multipartCmd.Args()) == 0 {
			log.Fatal("s3 uri is required")
		} else {
			s3path := multipartCmd.Arg(0)
			ListMultiParts(s3path)
		}
	case "abortmp":
		abortmpCmd.Parse(os.Args[2:])
		if len(abortmpCmd.Args()) == 0 {
			log.Fatal("s3 uri is required")
		} else {
			s3path := abortmpCmd.Arg(0)
			uploadId := abortmpCmd.Arg(1)
			AbortMultiPart(s3path, uploadId)
		}
	case "listmp":
		listmpCmd.Parse(os.Args[2:])
		if len(listmpCmd.Args()) == 0 {
			log.Fatal("s3 uri is required")
		} else {
			s3path := listmpCmd.Arg(0)
			uploadId := listmpCmd.Arg(1)
			MultiPartDetail(s3path, uploadId)
		}
	default:
		fmt.Println("expected subcommands to run")
		os.Exit(1)
	}
}
