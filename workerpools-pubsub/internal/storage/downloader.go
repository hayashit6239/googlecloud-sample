package storage

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

type Downloader struct {
	client     *storage.Client
	bucketName string
}

func NewDownloader(bucketName string) (*Downloader, error) {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage client: %v", err)
	}

	return &Downloader{
		client:     client,
		bucketName: bucketName,
	}, nil
}

func (d *Downloader) Close() error {
	return d.client.Close()
}

func (d *Downloader) DownloadDirectory(ctx context.Context, gcsPath, localDir string) error {
	bucket := d.client.Bucket(d.bucketName)
	
	prefix := gcsPath
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	it := bucket.Objects(ctx, &storage.Query{Prefix: prefix})
	
	var downloadedFiles []string
	
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to list objects: %v", err)
		}

		relativePath := strings.TrimPrefix(attrs.Name, prefix)
		if relativePath == "" {
			continue
		}

		localPath := filepath.Join(localDir, relativePath)
		
		if err := os.MkdirAll(filepath.Dir(localPath), 0755); err != nil {
			return fmt.Errorf("failed to create directory: %v", err)
		}

		obj := bucket.Object(attrs.Name)
		reader, err := obj.NewReader(ctx)
		if err != nil {
			return fmt.Errorf("failed to create reader for %s: %v", attrs.Name, err)
		}

		file, err := os.Create(localPath)
		if err != nil {
			reader.Close()
			return fmt.Errorf("failed to create file %s: %v", localPath, err)
		}

		if _, err := io.Copy(file, reader); err != nil {
			file.Close()
			reader.Close()
			return fmt.Errorf("failed to copy data to %s: %v", localPath, err)
		}

		file.Close()
		reader.Close()

		downloadedFiles = append(downloadedFiles, localPath)
		log.Printf("Downloaded: %s -> %s", attrs.Name, localPath)
	}

	log.Printf("Downloaded files in directory %s:", localDir)
	for _, file := range downloadedFiles {
		log.Printf("  - %s", file)
	}

	return nil
}