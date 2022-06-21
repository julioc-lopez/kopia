//go:build darwin || (linux && amd64)
// +build darwin linux,amd64

package snapmeta

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"sync"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/filesystem"
	"github.com/kopia/kopia/repo/blob/s3"
	"github.com/kopia/kopia/tests/robustness"
	"github.com/kopia/kopia/tests/tools/kopiaclient"
	"github.com/pkg/errors"
)

const (
	awsAccessKeyIDEnvKey     = "AWS_ACCESS_KEY_ID"
	awsSecretAccessKeyEnvKey = "AWS_SECRET_ACCESS_KEY" //nolint:gosec
	s3Endpoint               = "s3.amazonaws.com"
	repoPassword             = "kj13498po&_EXAMPLE" //nolint:gosec
)

// KopiaPersisterLight is a wrapper for KopiaClient that satisfies the Persister
// interface.
type KopiaPersisterLight struct {
	kc            *kopiaclient.KopiaClient
	keysInProcess map[string]bool
	c             *sync.Cond
	baseDir       string
}

var _ robustness.Persister = (*KopiaPersisterLight)(nil)

// NewPersisterLight returns a new KopiaPersisterLight.
func NewPersisterLight(baseDir string) (*KopiaPersisterLight, error) {
	persistenceDir, err := os.MkdirTemp(baseDir, "kopia-persistence-root-")
	if err != nil {
		return nil, err
	}

	configFile := filepath.Join(persistenceDir, "repository.config")

	return &KopiaPersisterLight{
		kc:            kopiaclient.NewKopiaClient(configFile, repoPassword),
		keysInProcess: map[string]bool{},
		c:             sync.NewCond(&sync.Mutex{}),
		baseDir:       persistenceDir,
	}, nil
}

// ConnectOrCreateRepo creates a new Kopia repo or connects to an existing one if possible.
func (kpl *KopiaPersisterLight) ConnectOrCreateRepo(repoPath string) error {
	st, err := getStorageFromEnvironment(context.Background(), repoPath)
	if err != nil {
		return err
	}

	return kpl.kc.ConnectOrCreate(context.Background(), repoPath, st)
}

// Store pushes the key value pair to the Kopia repository.
func (kpl *KopiaPersisterLight) Store(ctx context.Context, key string, val []byte) error {
	kpl.waitFor(key)
	defer kpl.doneWith(key)

	log.Println("pushing metadata for", key)

	return kpl.kc.SnapshotCreate(ctx, key, val)
}

// Load pulls the key value pair from the Kopia repo and returns the value.
func (kpl *KopiaPersisterLight) Load(ctx context.Context, key string) ([]byte, error) {
	kpl.waitFor(key)
	defer kpl.doneWith(key)

	log.Println("pulling metadata for", key)

	return kpl.kc.SnapshotRestore(ctx, key)
}

// Delete deletes all snapshots associated with the given key.
func (kpl *KopiaPersisterLight) Delete(ctx context.Context, key string) error {
	kpl.waitFor(key)
	defer kpl.doneWith(key)

	log.Println("deleting metadata for", key)

	return kpl.kc.SnapshotDelete(ctx, key)
}

// LoadMetadata is a no-op. It is included to satisfy the Persister interface.
func (kpl *KopiaPersisterLight) LoadMetadata() error {
	return nil
}

// FlushMetadata is a no-op. It is included to satisfy the Persister interface.
func (kpl *KopiaPersisterLight) FlushMetadata() error {
	return nil
}

// GetPersistDir returns the persistence directory.
func (kpl *KopiaPersisterLight) GetPersistDir() string {
	return kpl.baseDir
}

// Cleanup removes the persistence directory and closes the Kopia repo.
func (kpl *KopiaPersisterLight) Cleanup() {
	if err := os.RemoveAll(kpl.baseDir); err != nil {
		log.Println("cannot remove persistence dir")
	}
}

func (kpl *KopiaPersisterLight) waitFor(key string) {
	kpl.c.L.Lock()
	for kpl.keysInProcess[key] {
		kpl.c.Wait()
	}

	kpl.keysInProcess[key] = true
	kpl.c.L.Unlock()
}

func (kpl *KopiaPersisterLight) doneWith(key string) {
	kpl.c.L.Lock()
	delete(kpl.keysInProcess, key)
	kpl.c.L.Unlock()
	kpl.c.Broadcast()
}

// Behavior: if bucket name is set, assume the storage is an S3-compatible
// backend, then create it and return it.
// Otherwise, assume it is a filesystem backend
func getStorageFromEnvironment(ctx context.Context, prefixPath string) (blob.Storage, error) {
	bucketName := os.Getenv(S3BucketNameEnvKey)
	if bucketName == "" {
		if err := os.MkdirAll(prefixPath, 0o700); err != nil {
			return nil, errors.Wrap(err, "cannot create directory")
		}

		fsOpts := &filesystem.Options{
			Path: prefixPath,
		}

		st, err := filesystem.New(ctx, fsOpts, false)

		return st, errors.Wrap(err, "cannot create FS storage")
	}

	// assume S3 otherwise
	s3Opts := &s3.Options{
		BucketName:      bucketName,
		Prefix:          prefixPath,
		Endpoint:        s3Endpoint,
		AccessKeyID:     os.Getenv(awsAccessKeyIDEnvKey),
		SecretAccessKey: os.Getenv(awsSecretAccessKeyEnvKey),
	}

	if s3Opts.AccessKeyID == "" || s3Opts.SecretAccessKey == "" {
		return nil, errors.New("S3 credentials must be specified in the " + awsAccessKeyIDEnvKey + " and " + awsSecretAccessKeyEnvKey + " environment variables")
	}

	st, err := s3.New(ctx, s3Opts)

	return st, errors.Wrap(err, "unable to create S3 storage")
}
