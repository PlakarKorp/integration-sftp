/*
 * Copyright (c) 2023 Gilles Chehade <gilles@poolp.org>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package importer

import (
	"io"
	"os"
	"path"
	"strings"
	"sync"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/snapshot/importer"
	"github.com/pkg/sftp"
)

type file struct {
	path string
	info os.FileInfo
}

// Worker pool to handle file scanning in parallel
func (p *SFTPImporter) walkDir_worker(jobs <-chan file, results chan<- *importer.ScanResult, wg *sync.WaitGroup) {
	defer wg.Done()

	for file := range jobs {
		fileinfo := objects.FileInfoFromStat(file.info)

		var originFile string
		var err error
		if fileinfo.Mode()&os.ModeSymlink != 0 {
			originFile, err = p.client.ReadLink(file.path)
			if err != nil {
				results <- importer.NewScanError(file.path, err)
				continue
			}
		}
		results <- importer.NewScanRecord(file.path, originFile, fileinfo, []string{},
			func() (io.ReadCloser, error) { return p.client.Open(file.path) })
	}
}

func (p *SFTPImporter) walkDir_addPrefixDirectories(jobs chan<- file, results chan<- *importer.ScanResult) {
	// Clean the directory and split the path into components
	directory := path.Clean(p.rootDir)
	atoms := strings.Split(directory, string(os.PathSeparator))

	for i := range len(atoms) - 1 {
		path := path.Join(atoms[0 : i+1]...)

		if !strings.HasPrefix(path, "/") {
			path = "/" + path
		}

		info, err := p.client.Stat(path)
		if err != nil {
			results <- importer.NewScanError(path, err)
			continue
		}

		jobs <- file{path: path, info: info}
	}
}

func (p *SFTPImporter) walkDir_walker(numWorkers int) (<-chan *importer.ScanResult, error) {
	results := make(chan *importer.ScanResult, 1000) // Larger buffer for results
	jobs := make(chan file, 1000)                    // Buffered channel to feed paths to workers
	var wg sync.WaitGroup

	rootInfo, err := p.client.Lstat(p.rootDir)
	if err != nil {
		return nil, err
	}

	// Launch worker pool
	for w := 1; w <= numWorkers; w++ {
		wg.Add(1)
		go p.walkDir_worker(jobs, results, &wg)
	}

	// Start walking the directory and sending file paths to workers
	go func() {
		defer close(jobs)

		if rootInfo.Mode()&os.ModeSymlink != 0 {
			originFile, err := p.client.ReadLink(p.rootDir)
			if err != nil {
				results <- importer.NewScanError(p.rootDir, err)
				return
			}

			if !path.IsAbs(originFile) {
				originFile = path.Join(path.Dir(p.rootDir), originFile)
			}

			results <- importer.NewScanRecord(p.rootDir, originFile,
				objects.FileInfoFromStat(rootInfo), nil, nil)

			p.rootDir = originFile
		}

		// Add prefix directories first
		p.walkDir_addPrefixDirectories(jobs, results)

		err = SFTPWalk(p.client, p.rootDir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				results <- importer.NewScanError(path, err)
				return nil
			}
			jobs <- file{path: path, info: info}
			return nil
		})
		if err != nil {
			results <- importer.NewScanError(p.rootDir, err)
		}
	}()

	// Close the results channel when all workers are done
	go func() {
		wg.Wait()
		close(results)
	}()

	return results, nil
}

func SFTPWalk(client *sftp.Client, remotePath string, walkFn func(path string, info os.FileInfo, err error) error) error {
	info, err := client.Lstat(remotePath)
	if err != nil {
		// If we can't stat the file, call walkFn with the error.
		return walkFn(remotePath, nil, err)
	}
	// Call the walk function for the current file/directory.
	if err := walkFn(remotePath, info, nil); err != nil {
		return err
	}

	// If it's not a directory, nothing more to do.
	if !info.IsDir() {
		return nil
	}
	// List the directory contents.
	entries, err := client.ReadDir(remotePath)
	if err != nil {
		return walkFn(remotePath, info, err)
	}
	// Recursively walk each entry.
	for _, entry := range entries {
		newPath := path.Join(remotePath, entry.Name()) // Use "path" since remote paths are POSIX style.
		if err := SFTPWalk(client, newPath, walkFn); err != nil {
			return err
		}
	}
	return nil
}
