/*
 * Copyright (c) 2025 Gilles Chehade <gilles@poolp.org>
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
	"context"
	"fmt"
	"net/url"
	"os"
	"path"
	"strconv"
	"sync"

	plakarsftp "github.com/PlakarKorp/integration-sftp/common"
	"github.com/PlakarKorp/kloset/connectors"
	"github.com/PlakarKorp/kloset/connectors/importer"
	"github.com/PlakarKorp/kloset/exclude"
	"github.com/PlakarKorp/kloset/location"
	"github.com/pkg/sftp"
)

func init() {
	importer.Register("sftp", 0, NewImporter)
}

type Importer struct {
	opts *connectors.Options

	client   *sftp.Client
	pool     *plakarsftp.ClientPool
	endpoint *url.URL

	rootDir   string
	realpath  string
	excludes  *exclude.RuleSet
	nocrossfs bool
	devno     uint64
	poolSize  int
}

func NewImporter(appCtx context.Context, opts *connectors.Options, name string, config map[string]string) (importer.Importer, error) {
	var err error

	target := config["location"]

	parsed, err := url.Parse(target)
	if err != nil {
		return nil, err
	}
	rootDir := parsed.Path

	nocrossfs, _ := strconv.ParseBool(config["dont_traverse_fs"])

	excludes := exclude.NewRuleSet()
	if err := excludes.AddRulesFromArray(opts.Excludes); err != nil {
		return nil, fmt.Errorf("failed to setup exclude rules: %w", err)
	}

	client, err := plakarsftp.Connect(parsed, config)
	if err != nil {
		return nil, err
	}

	poolSize, err := clientPoolSize(opts, config)
	if err != nil {
		_ = client.Close()
		return nil, err
	}

	pool, err := plakarsftp.NewClientPool(parsed, config, poolSize)
	if err != nil {
		_ = client.Close()
		return nil, err
	}

	imp := &Importer{
		opts:      opts,
		endpoint:  parsed,
		client:    client,
		pool:      pool,
		nocrossfs: nocrossfs,
		rootDir:   rootDir,
		excludes:  excludes,
		poolSize:  poolSize,
	}

	realpath, devno, err := imp.realpathFollow(parsed.Path)
	if err != nil {
		pool.Close()
		_ = client.Close()
		return nil, err
	}
	imp.realpath = realpath
	imp.devno = devno

	return imp, nil
}

func (imp *Importer) Type() string {
	return "sftp"
}

func (imp *Importer) Origin() string {
	return imp.endpoint.Host
}

func (imp *Importer) Root() string {
	return imp.rootDir
}

func (imp *Importer) Flags() location.Flags {
	return 0
}

func (imp *Importer) Import(ctx context.Context, records chan<- *connectors.Record, results <-chan *connectors.Result) error {
	defer close(records)
	return imp.walkDir_walker(ctx, records, imp.poolSize)
}

func (imp *Importer) walkDir_walker(ctx context.Context, records chan<- *connectors.Record, numWorkers int) error {
	if numWorkers <= 0 {
		numWorkers = plakarsftp.DefaultPoolSize
	}

	jobs := make(chan file, numWorkers*4) // Buffered channel to feed paths to workers
	var wg sync.WaitGroup
	for range numWorkers {
		wg.Add(1)
		go imp.walkDir_worker(ctx, jobs, records, &wg)
	}

	// Add prefix directories first
	imp.walkDir_addPrefixDirectories(path.Dir(imp.realpath), records)
	if imp.realpath != imp.Root() {
		imp.walkDir_addPrefixDirectories(imp.Root(), records)
	}

	err := SFTPWalk(imp.client, imp.rootDir, func(path string, info os.FileInfo, err error) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		if err != nil {
			records <- connectors.NewError(path, err)
			return nil
		}

		if path != "/" {
			if imp.excludes.IsExcluded(path, info.IsDir()) {
				return SkipDir
			}
		}

		if info.IsDir() && imp.nocrossfs {
			same := isSameFs(imp.devno, info)
			if !same {
				return SkipDir
			}
		}

		select {
		case jobs <- file{path: path, info: info}:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	close(jobs)
	wg.Wait()
	return err
}

func (imp *Importer) realpathFollow(target string) (resolved string, dev uint64, err error) {
	info, err := imp.client.Lstat(target)
	if err != nil {
		return
	}

	if info.Mode()&os.ModeDir != 0 {
		dev = dirDevice(info)
	}

	if info.Mode()&os.ModeSymlink != 0 {
		realpath, err := os.Readlink(target)
		if err != nil {
			return "", 0, err
		}

		if !path.IsAbs(realpath) {
			realpath = path.Join(path.Dir(target), realpath)
		}
		target = realpath
	}

	return target, dev, nil
}

func (p *Importer) Ping(ctx context.Context) error {
	_, err := p.client.Lstat(p.rootDir)
	return err
}

func (p *Importer) Close(ctx context.Context) error {
	if p.pool != nil {
		p.pool.Close()
	}
	if p.client != nil {
		return p.client.Close()
	}
	return nil
}

func clientPoolSize(opts *connectors.Options, config map[string]string) (int, error) {
	if value := config["sftp_concurrency"]; value != "" {
		size, err := strconv.Atoi(value)
		if err != nil {
			return 0, fmt.Errorf("invalid sftp_concurrency: %w", err)
		}
		return size, nil
	}

	if opts != nil && opts.MaxConcurrency > 0 {
		return opts.MaxConcurrency, nil
	}

	return plakarsftp.DefaultPoolSize, nil
}
