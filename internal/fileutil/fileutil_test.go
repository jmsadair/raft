package fileutil

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestRemoveTmpFiles checks that RemoveTmpFiles will only remove temporary files and directories.
func TestRemoveTmpFiles(t *testing.T) {
	rootDir := t.TempDir()

	// Create a temporary file and directory. Both should be removed.
	tmpFile, err := os.CreateTemp(rootDir, "tmp-file-test")
	require.NoError(t, err)
	tmpDir, err := os.MkdirTemp(rootDir, "tmp-dir-test")
	require.NoError(t, err)

	// Create a non-temporary file and directory. Both should not be removed.
	file, err := os.Create("file-test")
	require.NoError(t, err)
	dir := filepath.Join(rootDir, "test-dir")
	require.NoError(t, os.Mkdir(dir, 0o666))

	require.NoError(t, RemoveTmpFiles(rootDir))

	// Check that the non-temporary directory and file exist.
	require.DirExists(t, dir)
	require.FileExists(t, file.Name())

	// Check that the temporary directory and file do no exist.
	require.NoDirExists(t, tmpDir)
	require.NoFileExists(t, tmpFile.Name())
}
