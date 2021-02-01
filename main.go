package main

import (
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

func getPath(path string) string {
	return strings.TrimPrefix(path, "/")
}

func main() {
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

	set_acl_public := setaclCmd.Bool("acl-public", false, "Store objects with ACL allowing read for anyone.")
	set_acl_private := setaclCmd.Bool("acl-private", false, "Store objects with default ACL allowing access for you only.")

	switch os.Args[1] {
	case "ls":
		lsCmd.Parse(os.Args[2:])
		fmt.Println(lsCmd.Args())
		if len(lsCmd.Args()) == 0 {
			svc, sess := CreateS3Client()
			ListS3Bucket(sess, svc)
		} else {
			svc, _ := CreateS3Client()
			s3path := lsCmd.Arg(0)
			s3url, err := url.Parse(s3path)
			if err != nil {
				log.Fatal(err)
			}
			if s3url.Scheme != "s3" {
				log.Fatal("path must be s3 url, like: s3://bucket")
			}
			bucket := s3url.Host
			path := strings.TrimPrefix(s3url.Path, "/")
			obj_result, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String(path), Delimiter: aws.String("/")})
			if err != nil {
				exitErrorf("Unable to list objects, %v", err)
			}
			log.Println("Objects:")

			for _, o := range obj_result.Contents {
				fmt.Printf("* %s %s %s\n",
					aws.StringValue(o.Key), aws.StringValue(o.ETag), aws.TimeValue(o.LastModified))
			}

			for _, o := range obj_result.CommonPrefixes {
				fmt.Printf("* %s\n",
					aws.StringValue(o.Prefix))
			}
		}
	case "get":
		getCmd.Parse(os.Args[2:])
		fmt.Println(getCmd.Args())
		if len(getCmd.Args()) != 2 {
			log.Fatal("source and dest is required")
		} else {
			_, sess := CreateS3Client()
			local_file_path := getCmd.Arg(1)
			s3url, err := url.Parse(getCmd.Arg(0))
			if err != nil {
				log.Fatal(err)
			}
			if s3url.Scheme != "s3" {
				log.Fatal("path must be s3 url, like: s3://bucket")
			}
			bucket := s3url.Host
			path := getPath(s3url.Path)

			file, err := os.Create(local_file_path)
			if err != nil {
				os.MkdirAll(filepath.Dir(local_file_path), os.ModePerm)
				file, err = os.Create(local_file_path)
				if err != nil {
					exitErrorf("Unable to open file %q, %v", local_file_path, err)
				}
			}
			defer file.Close()

			downloader := s3manager.NewDownloader(sess)
			numBytes, err := downloader.Download(file,
				&s3.GetObjectInput{
					Bucket: aws.String(bucket),
					Key:    &path,
				})
			if err != nil {
				exitErrorf("Unable to download item %q, %v", local_file_path, err)
			}

			fmt.Println("Downloaded", file.Name(), numBytes, "bytes")
		}
	case "put":
		putCmd.Parse(os.Args[2:])
		if len(putCmd.Args()) != 2 {
			log.Fatal("source and dest is required")
		} else {
			_, sess := CreateS3Client()
			local_file_path := putCmd.Arg(0)
			s3url, err := url.Parse(putCmd.Arg(1))
			if err != nil {
				log.Fatal(err)
			}
			if s3url.Scheme != "s3" {
				log.Fatal("path must be s3 url, like: s3://bucket")
			}
			bucket := s3url.Host
			targetPath := getPath(s3url.Path)

			uploader := s3manager.NewUploader(sess)
			file, err := os.Open(local_file_path)
			if err != nil {
				log.Println("Failed opening file", local_file_path, err)
			}
			defer file.Close()
			result, err := uploader.Upload(&s3manager.UploadInput{
				Bucket: &bucket,
				Key:    aws.String(targetPath),
				Body:   file,
			})
			if err != nil {
				log.Fatalln("Failed to upload", local_file_path, err)
			}
			log.Println("Uploaded", local_file_path, result.Location)
		}
	case "rm":
		rmCmd.Parse(os.Args[2:])
		fmt.Println(rmCmd.Args())
		if len(rmCmd.Args()) == 0 {
			log.Fatal("s3 uri is required")
		} else {
			svc, _ := CreateS3Client()
			s3url, err := url.Parse(rmCmd.Arg(0))
			if err != nil {
				log.Fatal(err)
			}
			if s3url.Scheme != "s3" {
				log.Fatal("path must be s3 url, like: s3://bucket")
			}
			bucket := s3url.Host
			targetPath := getPath(s3url.Path)

			result, err := svc.DeleteObject(&s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(targetPath)})
			if err != nil {
				exitErrorf("Unable to list objects, %v, %s, %s", err)
			}
			log.Println(result)
		}
	case "info":
		infoCmd.Parse(os.Args[2:])
		if len(infoCmd.Args()) == 0 {
			log.Fatal("s3 uri is required")
		} else {
			svc, _ := CreateS3Client()
			s3url, err := url.Parse(infoCmd.Arg(0))
			if err != nil {
				log.Fatal(err)
			}
			if s3url.Scheme != "s3" {
				log.Fatal("path must be s3 url, like: s3://bucket")
			}
			bucket := s3url.Host
			targetPath := getPath(s3url.Path)

			acl_result, err := svc.GetObjectAcl(&s3.GetObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(targetPath)})
			if err != nil {
				exitErrorf("Unable to list objects, %v, %s, %s", err)
			}
			log.Println(acl_result)

			result, err := svc.HeadObject(&s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(targetPath)})
			if err != nil {
				exitErrorf("Unable to list objects, %v, %s, %s", err)
			}
			log.Println(result)
		}
	case "cp":
		cpCmd.Parse(os.Args[2:])
		if len(cpCmd.Args()) != 2 {
			log.Fatal("source and dest is required")
		} else {
			svc, _ := CreateS3Client()
			srcurl, err := url.Parse(cpCmd.Arg(0))
			if err != nil {
				log.Fatal(err)
			}
			if srcurl.Scheme != "s3" {
				log.Fatal("path must be s3 url, like: s3://bucket")
			}
			srcbucket := srcurl.Host
			srcpath := getPath(srcurl.Path)

			dsturl, err := url.Parse(cpCmd.Arg(0))
			if err != nil {
				log.Fatal(err)
			}
			if srcurl.Scheme != "s3" {
				log.Fatal("path must be s3 url, like: s3://bucket")
			}
			dstbucket := dsturl.Host
			dstpath := getPath(dsturl.Path)

			if srcbucket != dstbucket {
				exitErrorf("source and dest must be in the same bucket")
			}

			result, err := svc.CopyObject(&s3.CopyObjectInput{Bucket: aws.String(dstbucket), CopySource: aws.String(srcpath), Key: aws.String(dstpath)})
			if err != nil {
				exitErrorf("Unable to list objects, %v, %s, %s", err)
			}
			log.Println(result)
		}
	case "modify":
		modifyCmd.Parse(os.Args[2:])
	case "setacl":
		setaclCmd.Parse(os.Args[2:])
		if len(setaclCmd.Args()) == 0 {
			log.Fatal("s3 uri is required")
		} else {
			svc, _ := CreateS3Client()
			s3url, err := url.Parse(setaclCmd.Arg(0))
			if err != nil {
				log.Fatal(err)
			}
			if s3url.Scheme != "s3" {
				log.Fatal("path must be s3 url, like: s3://bucket")
			}
			bucket := s3url.Host
			targetPath := getPath(s3url.Path)

			var permission string
			if *set_acl_public {
				permission = "public-read"
			} else if *set_acl_private {
				permission = "private"
			} else {
				permission = "private"
			}

			result, err := svc.PutObjectAcl(&s3.PutObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(targetPath), ACL: aws.String(permission)})
			if err != nil {
				exitErrorf("Unable to list objects, %v, %s, %s", err)
			}
			log.Println(result)
		}
	case "sync":
		syncCmd.Parse(os.Args[2:])
		if len(getCmd.Args()) != 2 {
			log.Fatal("source and dest is required")
		} else {
			srcurl, err := url.Parse(setaclCmd.Arg(0))
			if err != nil {
				log.Fatal(err)
			}
			dsturl, err := url.Parse(setaclCmd.Arg(1))
			if err != nil {
				log.Fatal(err)
			}

			svc, sess := CreateS3Client()
			if srcurl.Scheme == "s3" && dsturl.Scheme == "s3" {
				log.Fatal("both source and dest are s3 address is not supported")
			} else if srcurl.Scheme == "s3" {
				DownloadS3Dir(sess, svc, setaclCmd.Arg(0), setaclCmd.Arg(1))
			} else if dsturl.Scheme == "s3" {
				UploadS3Dir(sess, svc, setaclCmd.Arg(0), setaclCmd.Arg(1))
			} else {
				log.Fatal("not supported")
			}

		}
	case "multipart":
		multipartCmd.Parse(os.Args[2:])
		if len(multipartCmd.Args()) == 0 {
			log.Fatal("s3 uri is required")
		} else {
			svc, _ := CreateS3Client()
			s3url, err := url.Parse(multipartCmd.Arg(0))
			if err != nil {
				log.Fatal(err)
			}
			if s3url.Scheme != "s3" {
				log.Fatal("path must be s3 url, like: s3://bucket")
			}
			bucket := s3url.Host

			result, err := svc.ListMultipartUploads(&s3.ListMultipartUploadsInput{Bucket: aws.String(bucket)})
			if err != nil {
				exitErrorf("Unable to list objects, %v, %s, %s", err)
			}
			log.Println(result)

			fmt.Println("Initiated\tPath\tId")
			for _, item := range result.Uploads {
				fmt.Println(item.Initiated, aws.StringValue(item.Key), aws.StringValue(item.UploadId))
			}
		}
	case "abortmp":
		abortmpCmd.Parse(os.Args[2:])
		if len(abortmpCmd.Args()) == 0 {
			log.Fatal("s3 uri is required")
		} else {
			svc, _ := CreateS3Client()
			s3url, err := url.Parse(abortmpCmd.Arg(0))
			if err != nil {
				log.Fatal(err)
			}
			if s3url.Scheme != "s3" {
				log.Fatal("path must be s3 url, like: s3://bucket")
			}
			bucket := s3url.Host
			path := getPath(s3url.Path)
			upload_id := abortmpCmd.Arg(1)

			result, err := svc.AbortMultipartUpload(&s3.AbortMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(path), UploadId: aws.String(upload_id)})
			if err != nil {
				exitErrorf("Unable to list objects, %v, %s, %s", err)
			}
			log.Println(result)
		}
	case "listmp":
		listmpCmd.Parse(os.Args[2:])
		if len(listmpCmd.Args()) == 0 {
			log.Fatal("s3 uri is required")
		} else {
			svc, _ := CreateS3Client()
			s3url, err := url.Parse(listmpCmd.Arg(0))
			if err != nil {
				log.Fatal(err)
			}
			if s3url.Scheme != "s3" {
				log.Fatal("path must be s3 url, like: s3://bucket")
			}
			bucket := s3url.Host
			path := getPath(s3url.Path)
			upload_id := listmpCmd.Arg(1)

			result, err := svc.ListParts(&s3.ListPartsInput{Bucket: aws.String(bucket), Key: aws.String(path), UploadId: aws.String(upload_id)})
			if err != nil {
				exitErrorf("Unable to list objects, %v, %s, %s", err)
			}
			log.Println(result)

			fmt.Println("LastModified\t\t\tPartNumber\tETag\tSize")
			for _, item := range result.Parts {
				fmt.Println(item.LastModified, aws.Int64Value(item.PartNumber), aws.StringValue(item.ETag), aws.Int64Value(item.Size))
			}
		}
	default:
		fmt.Println("expected subcommands to run")
		os.Exit(1)
	}
}
