package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type fileWalk chan string

func (f fileWalk) Walk(path string, info os.FileInfo, err error) error {
	if err != nil {
		return err
	}
	if !info.IsDir() {
		f <- path
	}
	return nil
}

func exitErrorf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}

func CreateS3Client() (*s3.S3, *session.Session) {
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("cn-northwest-1")},
	)
	if err != nil {
		panic(err)
	}
	awsEndpoint := os.Getenv("AWS_ENDPOINT")
	svc := s3.New(sess, &aws.Config{Endpoint: aws.String(awsEndpoint)})
	return svc, sess
}

func ListBuckets() {
	svc, _ := CreateS3Client()
	result, err := svc.ListBuckets(nil)
	if err != nil {
		exitErrorf("Unable to list buckets, %v", err)
	}

	fmt.Println("Buckets:")
	for _, b := range result.Buckets {
		fmt.Printf("* %s created on %s\n",
			aws.StringValue(b.Name), aws.TimeValue(b.CreationDate))
	}
}

func UploadS3Dir(sess *session.Session, svc *s3.S3, src *url.URL, dst *url.URL) {
	bucket := dst.Host
	targetPath := strings.TrimPrefix(dst.Path, "/")
	fmt.Println("o", src, dst, bucket, targetPath)

	walker := make(fileWalk)
	go func() {
		// Gather the files to upload by walking the path recursively
		if err := filepath.Walk(src.Path, walker.Walk); err != nil {
			log.Fatalln("Walk failed:", err)
		}
		close(walker)
	}()

	// For each file found walking, upload it to S3
	uploader := s3manager.NewUploader(sess)
	for path := range walker {
		rel, err := filepath.Rel(src.Path, path)
		if err != nil {
			log.Fatalln("Unable to get relative path:", path, err)
		}
		file, err := os.Open(path)
		if err != nil {
			log.Println("Failed opening file", path, err)
			continue
		}
		result, err := uploader.Upload(&s3manager.UploadInput{
			Bucket: &bucket,
			Key:    aws.String(filepath.Join(targetPath, rel)),
			Body:   file,
		})
		if err != nil {
			log.Fatalln("Failed to upload", path, err)
		}
		log.Println("Uploaded", path, result.Location)
		file.Close()
	}
}

func DownloadS3Dir(sess *session.Session, svc *s3.S3, src *url.URL, dst *url.URL) {
	bucket := src.Host
	path := strings.TrimPrefix(src.Path, "/")
	result, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String(path), Delimiter: aws.String("")})
	if err != nil {
		exitErrorf("Unable to list objects, %v", err)
	}
	log.Println("Objects:")
	downloader := s3manager.NewDownloader(sess)

	for _, o := range result.Contents {
		fmt.Printf("* %s %s %s\n",
			aws.StringValue(o.Key), aws.StringValue(o.ETag), aws.TimeValue(o.LastModified))

		item := aws.StringValue(o.Key)
		item = strings.TrimPrefix(item, path)
		item = filepath.Join(dst.Path, item)
		file, err := os.Create(item)
		if err != nil {
			os.MkdirAll(filepath.Dir(item), os.ModePerm)
			file, err = os.Create(item)
			if err != nil {
				exitErrorf("Unable to open file %q, %v", item, err)
			}
		}

		numBytes, err := downloader.Download(file,
			&s3.GetObjectInput{
				Bucket: aws.String(bucket),
				Key:    o.Key,
			})
		if err != nil {
			exitErrorf("Unable to download item %q, %v", item, err)
		}

		fmt.Println("Downloaded", file.Name(), numBytes, "bytes")
		file.Close()
	}
}

func RemoveObject(s3path string) {
	svc, _ := CreateS3Client()
	s3url, err := url.Parse(s3path)
	if err != nil {
		log.Fatal(err)
	}
	if s3url.Scheme != "s3" {
		log.Fatal("path must be s3 url, like: s3://bucket")
	}
	bucket := s3url.Host
	targetPath := getPath(s3url.Path)

	_, err = svc.DeleteObject(&s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(targetPath)})
	if err != nil {
		exitErrorf("Unable to perform operations, %v", err)
	}
	fmt.Println("ok")
}

func ObjectInfo(s3path string) {
	svc, _ := CreateS3Client()
	s3url, err := url.Parse(s3path)
	if err != nil {
		log.Fatal(err)
	}
	if s3url.Scheme != "s3" {
		log.Fatal("path must be s3 url, like: s3://bucket")
	}
	bucket := s3url.Host
	targetPath := getPath(s3url.Path)

	aclResult, err := svc.GetObjectAcl(&s3.GetObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(targetPath)})
	if err != nil {
		checkError(err)
	}

	objResult, err := svc.HeadObject(&s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(targetPath)})
	if err != nil {
		checkError(err)
	}

	// todo separate message printing

	fmt.Printf("%12s:\t%s\n", "Object", s3path)
	fmt.Printf("%12s:\t%s%s\n", "URL", svc.Endpoint, s3url.Path) // todo error url should include bucket name
	fmt.Printf("%12s:\t%d\n", "Size", aws.Int64Value(objResult.ContentLength))
	fmt.Printf("%12s:\t%s\n", "Last Mod", objResult.LastModified)
	fmt.Printf("%12s:\t%s\n", "MIME Type", aws.StringValue(objResult.ContentType))
	fmt.Printf("%12s:\t%s\n", "MD5", unmarkEtag(*objResult.ETag))
	for _, i := range aclResult.Grants {
		var grantee string
		if aws.StringValue(i.Grantee.Type) == "CanonicalUser" {
			grantee = aws.StringValue(i.Grantee.ID)
		} else if aws.StringValue(i.Grantee.Type) == "Group" {
			grantee = aws.StringValue(i.Grantee.URI)
		} else {
			grantee = ""
		}
		fmt.Printf("%12s:\t%s: %s\n", "ACL", grantee, aws.StringValue(i.Permission))
	}
}

func checkError(err error) {
	if aerr, ok := err.(awserr.Error); ok {
		switch aerr.Code() {
		case s3.ErrCodeNoSuchKey:
			exitErrorf("%s: %s", s3.ErrCodeNoSuchKey, aerr.Message())
		default:
			exitErrorf("aws error: %s", aerr.Error())
		}
	} else {
		exitErrorf("other error: %s", aerr.Error())
	}
}

func ListObjects(s3path string) {
	svc, _ := CreateS3Client()
	s3url, err := url.Parse(s3path)
	if err != nil {
		log.Fatal(err)
	}
	if s3url.Scheme != "s3" {
		log.Fatal("path must be s3 url, like: s3://bucket")
	}
	bucket := s3url.Host
	path := strings.TrimPrefix(s3url.Path, "/")
	result, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String(path), Delimiter: aws.String("/")})
	if err != nil {
		exitErrorf("Unable to list objects, %v", err)
	}

	for _, o := range result.CommonPrefixes {
		fmt.Printf("s3://%s/%s\n", bucket,
			aws.StringValue(o.Prefix))
	}

	for _, o := range result.Contents {
		fmt.Printf("%s %s %d s3://%s/%s\n", aws.TimeValue(o.LastModified), unmarkEtag(aws.StringValue(o.ETag)), aws.Int64Value(o.Size),
			bucket, aws.StringValue(o.Key))
	}
}

func GetObject(s3Path string, localPath string) {
	_, sess := CreateS3Client()
	s3url, err := url.Parse(s3Path)
	if err != nil {
		log.Fatal(err)
	}
	if s3url.Scheme != "s3" {
		log.Fatal("path must be s3 url, like: s3://bucket")
	}
	bucket := s3url.Host
	path := getPath(s3url.Path)

	file, err := os.Create(localPath)
	if err != nil {
		err = os.MkdirAll(filepath.Dir(localPath), os.ModePerm)
		if err != nil {
			exitErrorf("Unable to create dir, %v", err)
		}

		file, err = os.Create(localPath)
		if err != nil {
			exitErrorf("Unable to open file %q, %v", localPath, err)
		}
	}

	downloader := s3manager.NewDownloader(sess)
	numBytes, err := downloader.Download(file,
		&s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    &path,
		})
	if err != nil {
		exitErrorf("Unable to download item %q, %v", localPath, err)
	}

	fmt.Println("Downloaded", file.Name(), numBytes, "bytes")
	file.Close()
}

func PutObject(s3Path string, localPath string) {
	_, sess := CreateS3Client()
	s3url, err := url.Parse(s3Path)
	if err != nil {
		log.Fatal(err)
	}
	if s3url.Scheme != "s3" {
		log.Fatal("path must be s3 url, like: s3://bucket")
	}
	bucket := s3url.Host
	targetPath := getPath(s3url.Path)

	uploader := s3manager.NewUploader(sess)
	file, err := os.Open(localPath)
	if err != nil {
		log.Println("Failed opening file", localPath, err)
	}
	defer file.Close()
	result, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: &bucket,
		Key:    aws.String(targetPath),
		Body:   file,
	})
	if err != nil {
		log.Fatalln("Failed to upload", localPath, err)
	}
	fmt.Printf("Uploaded %s s3://%s/%s at %s", localPath, bucket, targetPath, result.Location)
}

func CopyObject(src string, dst string) {
	svc, _ := CreateS3Client()
	srcUrl, err := url.Parse(src)
	if err != nil {
		log.Fatal(err)
	}
	if srcUrl.Scheme != "s3" {
		log.Fatal("path must be s3 url, like: s3://bucket")
	}
	srcBucket := srcUrl.Host
	srcPath := getPath(srcUrl.Path)

	dstUrl, err := url.Parse(dst)
	if err != nil {
		log.Fatal(err)
	}
	if srcUrl.Scheme != "s3" {
		log.Fatal("path must be s3 url, like: s3://bucket")
	}
	dstBucket := dstUrl.Host
	dstPath := getPath(dstUrl.Path)

	if srcBucket != dstBucket {
		exitErrorf("source and dest must be in the same bucket")
	}

	srcObject := fmt.Sprintf("/%s/%s", srcBucket, srcPath)
	result, err := svc.CopyObject(&s3.CopyObjectInput{Bucket: aws.String(dstBucket), CopySource: aws.String(srcObject), Key: aws.String(dstPath)})
	if err != nil {
		exitErrorf("Unable to perform operations, %v", err)
	}
	fmt.Println(result.CopyObjectResult)
}

func SetaclObject(s3path string, setPublic *bool, setPrivate *bool) {
	svc, _ := CreateS3Client()
	s3url, err := url.Parse(s3path)
	if err != nil {
		log.Fatal(err)
	}
	if s3url.Scheme != "s3" {
		log.Fatal("path must be s3 url, like: s3://bucket")
	}
	bucket := s3url.Host
	targetPath := getPath(s3url.Path)

	var permission string
	if *setPublic {
		permission = "public-read"
	} else if *setPrivate {
		permission = "private"
	} else {
		permission = "private"
	}
	fmt.Println(permission)
	_, err = svc.PutObjectAcl(&s3.PutObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(targetPath), ACL: aws.String(permission)})
	if err != nil {
		exitErrorf("Unable to set object acl, %v, %s, %s", err)
	}
	fmt.Println("ok")
}

func SyncDir(src string, dst string) {
	srcUrl, err := url.Parse(src)
	if err != nil {
		log.Fatal(err)
	}
	dstUrl, err := url.Parse(dst)
	if err != nil {
		log.Fatal(err)
	}

	svc, sess := CreateS3Client()
	if srcUrl.Scheme == "s3" && dstUrl.Scheme == "s3" {
		log.Fatal("both source and dest are s3 address is not supported")
	} else if srcUrl.Scheme == "s3" {
		DownloadS3Dir(sess, svc, srcUrl, dstUrl)
	} else if dstUrl.Scheme == "s3" {
		UploadS3Dir(sess, svc, srcUrl, dstUrl)
	} else {
		log.Fatal("not supported")
	}
}

func ListMultiParts(s3path string) {
	svc, _ := CreateS3Client()
	s3url, err := url.Parse(s3path)
	if err != nil {
		log.Fatal(err)
	}
	if s3url.Scheme != "s3" {
		log.Fatal("path must be s3 url, like: s3://bucket")
	}
	bucket := s3url.Host

	result, err := svc.ListMultipartUploads(&s3.ListMultipartUploadsInput{Bucket: aws.String(bucket)})
	if err != nil {
		exitErrorf("Unable to perform operations, %v", err)
	}

	fmt.Println("Initiated\tPath\tId")
	for _, item := range result.Uploads {
		fmt.Println(item.Initiated, aws.StringValue(item.Key), aws.StringValue(item.UploadId))
	}
}

func AbortMultiPart(s3path string, uploadId string) {
	s3url, err := url.Parse(s3path)
	svc, _ := CreateS3Client()
	if err != nil {
		log.Fatal(err)
	}
	if s3url.Scheme != "s3" {
		log.Fatal("path must be s3 url, like: s3://bucket")
	}
	bucket := s3url.Host
	path := getPath(s3url.Path)

	_, err = svc.AbortMultipartUpload(&s3.AbortMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(path), UploadId: aws.String(uploadId)})
	if err != nil {
		exitErrorf("Unable to perform operations, %v", err)
	}
	fmt.Println("ok")
}

func MultiPartDetail(s3path string, uploadId string) {
	svc, _ := CreateS3Client()
	s3url, err := url.Parse(s3path)
	if err != nil {
		log.Fatal(err)
	}
	if s3url.Scheme != "s3" {
		log.Fatal("path must be s3 url, like: s3://bucket")
	}
	bucket := s3url.Host
	path := getPath(s3url.Path)

	if path == "" {
		exitErrorf("Object key must be specified")
	}

	result, err := svc.ListParts(&s3.ListPartsInput{Bucket: aws.String(bucket), Key: aws.String(path), UploadId: aws.String(uploadId)})
	if err != nil {
		exitErrorf("Unable to perform operations, %v", err)
	}

	fmt.Println("LastModified\t\t\tPartNumber\tETag\tSize")
	for _, item := range result.Parts {
		fmt.Println(item.LastModified, aws.Int64Value(item.PartNumber), aws.StringValue(item.ETag), aws.Int64Value(item.Size))
	}
}
