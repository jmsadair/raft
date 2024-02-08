package fileutil

import (
	"os"
	"path/filepath"
	"strings"
)

// RemoveTmpFiles will remove all files in the root directory
// and its sub-directories with a name that has a 'tmp' prefix.
func RemoveTmpFiles(rootDir string) error {
	return filepath.Walk(rootDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !strings.HasPrefix(info.Name(), "tmp") {
			return nil
		}
		return os.RemoveAll(path)
	})
}
