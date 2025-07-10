package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"workerpools-pubsub/internal/pubsub"
	"workerpools-pubsub/internal/storage"
)

func main() {
	projectID := os.Getenv("GOOGLE_CLOUD_PROJECT")
	if projectID == "" {
		log.Fatal("GOOGLE_CLOUD_PROJECT environment variable is required")
	}

	subscriptionID := os.Getenv("PUBSUB_SUBSCRIPTION_ID")
	if subscriptionID == "" {
		log.Fatal("PUBSUB_SUBSCRIPTION_ID environment variable is required")
	}

	bucketName := os.Getenv("STORAGE_BUCKET_NAME")
	if bucketName == "" {
		log.Fatal("STORAGE_BUCKET_NAME environment variable is required")
	}

	downloadDir := os.Getenv("DOWNLOAD_DIR")
	if downloadDir == "" {
		downloadDir = "/tmp/downloads"
	}

	log.Printf("Starting worker with:")
	log.Printf("  Project ID: %s", projectID)
	log.Printf("  Subscription ID: %s", subscriptionID)
	log.Printf("  Bucket Name: %s", bucketName)
	log.Printf("  Download Directory: %s", downloadDir)

	subscriber, err := pubsub.NewSubscriber(projectID, subscriptionID)
	if err != nil {
		log.Fatalf("Failed to create subscriber: %v", err)
	}
	defer subscriber.Close()

	downloader, err := storage.NewDownloader(bucketName)
	if err != nil {
		log.Fatalf("Failed to create downloader: %v", err)
	}
	defer downloader.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt, syscall.SIGTERM)
		<-c
		log.Println("Received signal, shutting down...")
		cancel()
	}()

	handler := func(ctx context.Context, msg pubsub.Message) error {
		log.Printf("Received message with path: %s", msg.Path)

		localDir := filepath.Join(downloadDir, filepath.Base(msg.Path)+"_"+time.Now().Format("20060102_150405"))
		
		if err := os.MkdirAll(localDir, 0755); err != nil {
			return err
		}

		if err := downloader.DownloadDirectory(ctx, msg.Path, localDir); err != nil {
			return err
		}

		// ダウンロード完了後にファイル一覧を出力
		if err := listDownloadedFiles(localDir); err != nil {
			log.Printf("Warning: Failed to list downloaded files: %v", err)
		}

		// /tmp/downloads全体の内容を出力
		if err := listAllDownloads(downloadDir); err != nil {
			log.Printf("Warning: Failed to list all downloads: %v", err)
		}

		log.Printf("Successfully processed message for path: %s", msg.Path)
		return nil
	}

	log.Println("Starting to receive messages...")
	if err := subscriber.Receive(ctx, handler); err != nil {
		log.Printf("Subscriber stopped: %v", err)
	}

	log.Println("Worker stopped")
}

func listDownloadedFiles(dir string) error {
	fmt.Printf("\n=== Downloaded files in %s ===\n", dir)
	
	return filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		
		relPath, err := filepath.Rel(dir, path)
		if err != nil {
			relPath = path
		}
		
		if info.IsDir() {
			fmt.Printf("[DIR]  %s/\n", relPath)
		} else {
			fmt.Printf("[FILE] %s (%d bytes)\n", relPath, info.Size())
		}
		
		return nil
	})
}

func listAllDownloads(downloadDir string) error {
	fmt.Printf("\n=== All contents in %s ===\n", downloadDir)
	
	entries, err := os.ReadDir(downloadDir)
	if err != nil {
		return err
	}
	
	for _, entry := range entries {
		if entry.IsDir() {
			fmt.Printf("[DIR]  %s/\n", entry.Name())
		} else {
			info, err := entry.Info()
			if err != nil {
				fmt.Printf("[FILE] %s (size unknown)\n", entry.Name())
			} else {
				fmt.Printf("[FILE] %s (%d bytes)\n", entry.Name(), info.Size())
			}
		}
	}
	
	return nil
}