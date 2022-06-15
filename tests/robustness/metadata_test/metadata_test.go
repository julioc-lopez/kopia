package metadata_test

import (
	"bytes"
	"encoding/json"
	"os"
	"testing"

	"github.com/kopia/kopia/cli"
	"github.com/kopia/kopia/tests/tools/kopiarunner"
	"github.com/stretchr/testify/require"
)

func TestEnv(t *testing.T) {
	kid := os.Getenv("AWS_ACCESS_KEY_ID")

	t.Log("key id:", kid)
}

func TestMetadataFile(t *testing.T) {
	fi, err := os.Stat("/mnt/tmp/robustness/metadata-store-latest")

	require.NoError(t, err)
	require.NotNil(t, fi)

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
