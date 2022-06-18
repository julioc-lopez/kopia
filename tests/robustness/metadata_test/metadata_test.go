package metadata_test

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/golang/protobuf/proto" //lint:ignore SA1019 needed due to FSWalker
	fspb "github.com/google/fswalker/proto/fswalker"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/kopia/kopia/cli"
	"github.com/kopia/kopia/tests/robustness/checker"
	"github.com/kopia/kopia/tests/robustness/snapmeta"
	"github.com/kopia/kopia/tests/tools/kopiarunner"
)

func TestEnv(t *testing.T) {
	kid := os.Getenv("AWS_ACCESS_KEY_ID")

	t.Log("key id:", kid)
}

func TestMetadataFile(t *testing.T) {
	const (
		dirname  = "/mnt/tmp/robustness"
		filename = dirname + "/metadata-store-latest"
	)

	fi, err := os.Stat(filename)

	require.NoError(t, err)
	require.NotNil(t, fi)

	f, err := os.Open(filename)
	require.NoError(t, err)
	require.NotNil(t, f)

	d := json.NewDecoder(f)

	var m snapmeta.Simple

	require.NoError(t, d.Decode(&m))

	snapMetaDir := filepath.Join(dirname, "out", "snap-meta")
	outdir := filepath.Dir(snapMetaDir)

	require.NoError(t, os.MkdirAll(snapMetaDir, 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(outdir, "engine"), 0o755))

	const (
		statsKey     = "cumulative-engine-stats"
		logsKey      = "engine-logs"
		snapIndexKey = "checker-snapID-index"
	)

	var sm checker.SnapshotMetadata

	for k, v := range m.Data {
		t.Log(k)
		if k == statsKey || k == logsKey || k == snapIndexKey {
			subDir := "engine"

			if k == snapIndexKey {
				subDir = ""
			}

			err = ioutil.WriteFile(filepath.Join(outdir, subDir, k)+".json", v, 0o444)
			require.NoError(t, err)
			continue
		}

		// assume it is snapshot metadata
		err = json.Unmarshal(v, &sm)
		require.NoError(t, err)

		snapDir := filepath.Join(snapMetaDir, k)
		err = os.MkdirAll(snapDir, 0o755)
		require.NoError(t, err)

		maybeWriteValidationData(t, snapDir, sm.ValidationData)

		sm.ValidationData = nil

		jb, err := json.Marshal(sm)
		require.NoError(t, err)
		require.NotNil(t, jb)

		err = ioutil.WriteFile(filepath.Join(snapDir, "meta.json"), jb, 0o444)
		require.NoError(t, err)
	}
}

func maybeWriteValidationData(t *testing.T, snapDir string, vd []byte) {
	t.Helper()

	if len(vd) == 0 {
		return
	}

	var w fspb.Walk

	require.NoError(t, proto.Unmarshal(vd, &w))

	b, err := protojson.Marshal(proto.MessageV2(&w))
	require.NoError(t, err)

	err = ioutil.WriteFile(filepath.Join(snapDir, "validation-data.json"), b, 0o444)
	require.NoError(t, err)
}

//lint:ignore U1000 test code only, not currently used
func writePBText(t *testing.T, filename string, fw *fspb.Walk) { // nolint:unused
	// write as PB text format
	f, err := os.Create(filename)

	require.NoError(t, err)
	require.NotNil(t, f)

	defer f.Close()

	wr := bufio.NewWriter(f)

	require.NoError(t, proto.MarshalText(wr, fw))
	require.NoError(t, wr.Flush())
}

//lint:ignore U1000 test code only, not currently used
func writePB(t *testing.T, filename string, vd []byte) {
	err := ioutil.WriteFile(filename, vd, 0o444)
	require.NoError(t, err)
}

func TestRepoConnect(t *testing.T) {
	krOpts := kopiarunner.RunnerOpts{
		ConfigDir: "/user/.config/kopia/",
	}

	kr, err := kopiarunner.NewRunnerWithOptions(krOpts)

	// TODO: add repo connect command
	require.NoError(t, err)
	require.NotNil(t, kr)

}

func TestSnapshotIterate(t *testing.T) {
	// TODO: iterate through all snapshots and list the top directory
	// - get root entry from manifest
	// -
	krOpts := kopiarunner.RunnerOpts{
		ConfigDir: "/user/.config/kopia/",
	}

	kr, err := kopiarunner.NewRunnerWithOptions(krOpts)

	require.NoError(t, err)
	require.NotNil(t, kr)

}

func TestUnifyMetaPath(t *testing.T) {
	// create a runner environment
	// allow specifying an existing config file
	krOpts := kopiarunner.RunnerOpts{
		ConfigDir: "/user/.config/kopia/",
	}

	kr, err := kopiarunner.NewRunnerWithOptions(krOpts)

	require.NoError(t, err)
	require.NotNil(t, kr)

	out, _, err := kr.RunBytes("snapshot", "list", "--json")
	require.NoError(t, err)

	var snapshots []cli.SnapshotManifest

	mustParseJSON(t, out, &snapshots)

	const (
		destUser   = "root"
		destHost   = "robustness"
		destPath   = "/engine/metadata"
		destSource = destUser + "@" + destHost + ":" + destPath
	)

	for _, s := range snapshots {
		if src := s.Source; src.Host != destHost || src.UserName != destUser || src.Path != destPath {
			t.Log("migrating", src.UserName, src.Host, src.Path)

			_, _, err = kr.RunBytes("snapshot", "move-history", src.UserName+"@"+src.Host+":"+src.Path, destSource)

			require.NoError(t, err)
		}

		t.Log("not migrating", s.Source.Path)
	}
}

// MustParseJSONLines parses the lines containing JSON into the provided object.
func mustParseJSON(t *testing.T, in []byte, v interface{}) {
	t.Helper()

	dec := json.NewDecoder(bytes.NewReader(in))
	dec.DisallowUnknownFields()

	if err := dec.Decode(v); err != nil {
		t.Fatalf("failed to parse JSON %s: %v", in, err)
	}
}
