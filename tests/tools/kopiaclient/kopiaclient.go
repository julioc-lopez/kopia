//go:build darwin || (linux && amd64)
// +build darwin linux,amd64

// Package kopiaclient provides a client to interact with a Kopia repo.
package kopiaclient

import (
	"bytes"
	"context"
	"io"
	"log"
	"sync/atomic"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/fs/virtualfs"
	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
	"github.com/kopia/kopia/snapshot/snapshotfs"
	"github.com/kopia/kopia/tests/robustness"
)

// KopiaClient uses a Kopia repo to create, restore, and delete snapshots.
type KopiaClient struct {
	configPath string
	password   string
}

const (
	dataFileName = "data"
)

// NewKopiaClient returns a new KopiaClient.
func NewKopiaClient(configFile, password string) *KopiaClient {
	return &KopiaClient{
		configPath: configFile,
		password:   password,
	}
}

// ConnectOrCreate creates a new Kopia repo or connects to an existing one if possible.
func (kc *KopiaClient) ConnectOrCreate(ctx context.Context, repoDir string, st blob.Storage) error {
	if err := repo.Initialize(ctx, st, &repo.NewRepositoryOptions{}, kc.password); err != nil {
		if !errors.Is(err, repo.ErrAlreadyInitialized) {
			return errors.Wrap(err, "repo is already initialized")
		}

		log.Println("connecting to existing repository")
	}

	if err := repo.Connect(ctx, kc.configPath, st, kc.password, &repo.ConnectOptions{}); err != nil {
		return errors.Wrap(err, "error connecting to repository")
	}

	return nil
}

// SnapshotCreate creates a snapshot for the given path.
func (kc *KopiaClient) SnapshotCreate(ctx context.Context, key string, val []byte) error {
	r, err := repo.Open(ctx, kc.configPath, kc.password, &repo.Options{})
	if err != nil {
		return errors.Wrap(err, "cannot open repository")
	}

	ctx, rw, err := r.NewWriter(ctx, repo.WriteSessionOptions{})
	if err != nil {
		return errors.Wrap(err, "cannot get new repository writer")
	}

	si := kc.getSourceInfoFromKey(r, key)

	policyTree, err := policy.TreeForSource(ctx, r, si)
	if err != nil {
		return errors.Wrap(err, "cannot get policy tree for source")
	}

	source := kc.getSourceForKeyVal(key, val)
	u := snapshotfs.NewUploader(rw)

	man, err := u.Upload(ctx, source, policyTree, si)
	if err != nil {
		return errors.Wrap(err, "cannot get manifest")
	}

	log.Printf("snapshotting %v", units.BytesStringBase10(atomic.LoadInt64(&man.Stats.TotalFileSize)))

	if _, err := snapshot.SaveSnapshot(ctx, rw, man); err != nil {
		return errors.Wrap(err, "cannot save snapshot")
	}

	if err := rw.Flush(ctx); err != nil {
		return err
	}

	return r.Close(ctx)
}

// SnapshotRestore restores the latest snapshot for the given path.
func (kc *KopiaClient) SnapshotRestore(ctx context.Context, key string) ([]byte, error) {
	r, err := repo.Open(ctx, kc.configPath, kc.password, &repo.Options{})
	if err != nil {
		return nil, errors.Wrap(err, "cannot open repository")
	}

	mans, err := kc.getSnapshotsFromKey(ctx, r, key)
	if err != nil {
		return nil, errors.Wrap(err, "cannot get snapshots from key")
	}

	man := kc.latestManifest(mans)
	rootOIDWithPath := man.RootObjectID().String() + "/" + dataFileName

	oid, err := snapshotfs.ParseObjectIDWithPath(ctx, r, rootOIDWithPath)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot parse object ID %s", rootOIDWithPath)
	}

	or, err := r.OpenObject(ctx, oid)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot open object %s", oid)
	}

	val, err := io.ReadAll(or)
	if err != nil {
		return nil, err
	}

	log.Printf("restored %v", units.BytesStringBase10(int64(len(val))))

	if err := r.Close(ctx); err != nil {
		return nil, err
	}

	return val, nil
}

// SnapshotDelete deletes all snapshots for a given path.
func (kc *KopiaClient) SnapshotDelete(ctx context.Context, key string) error {
	r, err := repo.Open(ctx, kc.configPath, kc.password, &repo.Options{})
	if err != nil {
		return errors.Wrap(err, "cannot open repository")
	}

	ctx, rw, err := r.NewWriter(ctx, repo.WriteSessionOptions{})
	if err != nil {
		return errors.Wrap(err, "cannot get new repository writer")
	}

	mans, err := kc.getSnapshotsFromKey(ctx, r, key)
	if err != nil {
		return errors.Wrap(err, "cannot get snapshots from key")
	}

	for _, man := range mans {
		if err := rw.DeleteManifest(ctx, man.ID); err != nil {
			return errors.Wrap(err, "cannot delete manifest")
		}
	}

	if err := rw.Flush(ctx); err != nil {
		return err
	}

	return r.Close(ctx)
}

// getSourceForKeyVal creates a virtual directory for `key` that contains a single virtual file that
// reads its contents from `val`.
func (kc *KopiaClient) getSourceForKeyVal(key string, val []byte) fs.Entry {
	return virtualfs.NewStaticDirectory(key, []fs.Entry{
		virtualfs.StreamingFileFromReader(dataFileName, bytes.NewReader(val)),
	})
}

func (kc *KopiaClient) getSnapshotsFromKey(ctx context.Context, r repo.Repository, key string) ([]*snapshot.Manifest, error) {
	si := kc.getSourceInfoFromKey(r, key)

	manifests, err := snapshot.ListSnapshots(ctx, r, si)
	if err != nil {
		return nil, errors.Wrap(err, "cannot list snapshots")
	}

	if len(manifests) == 0 {
		return nil, robustness.ErrKeyNotFound
	}

	return manifests, nil
}

func (kc *KopiaClient) getSourceInfoFromKey(r repo.Repository, key string) snapshot.SourceInfo {
	return snapshot.SourceInfo{
		Host:     r.ClientOptions().Hostname,
		UserName: r.ClientOptions().Username,
		Path:     key,
	}
}

func (kc *KopiaClient) latestManifest(mans []*snapshot.Manifest) *snapshot.Manifest {
	latest := mans[0]

	for _, m := range mans {
		if m.StartTime.After(latest.StartTime) {
			latest = m
		}
	}

	return latest
}
