package common

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"sync"

	"github.com/pkg/sftp"
)

const DefaultPoolSize = 5

type ClientPool struct {
	clients chan *sftp.Client
	// clientSlots limits total clients, including idle, leased, and in-flight creations.
	// to adhere to the windowing of max concurrency
	clientSlots chan struct{}
	done        chan struct{}

	endpoint *url.URL
	params   map[string]string

	mu     sync.Mutex
	once   sync.Once
	closed bool
}

type ClientLease struct {
	*sftp.Client

	pool *ClientPool
	once sync.Once
}

type LeasedReadCloser struct {
	io.ReadCloser

	lease *ClientLease
}

func NewClientPool(endpoint *url.URL, params map[string]string, size int) (*ClientPool, error) {
	if size <= 0 {
		size = DefaultPoolSize
	}

	pool := &ClientPool{
		clients:     make(chan *sftp.Client, size),
		clientSlots: make(chan struct{}, size),
		done:        make(chan struct{}),
		endpoint:    endpoint,
		params:      cloneParams(params),
	}

	return pool, nil
}

func (p *ClientPool) Lease(ctx context.Context) (*ClientLease, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	select {
	case client := <-p.clients:
		return p.wrap(client)
	default:
	}

	select {
	case client := <-p.clients:
		return p.wrap(client)
	case p.clientSlots <- struct{}{}:
		if p.isClosed() {
			p.releaseClientSlot()
			return nil, fmt.Errorf("sftp client pool is closed")
		}
		client, err := Connect(p.endpoint, p.params)
		if err != nil {
			p.releaseClientSlot()
			return nil, fmt.Errorf("create sftp client lease: %w", err)
		}
		return p.wrap(client)
	case <-p.done:
		return nil, fmt.Errorf("sftp client pool is closed")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (p *ClientPool) isClosed() bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.closed
}

func (p *ClientPool) wrap(client *sftp.Client) (*ClientLease, error) {
	if client == nil {
		return nil, fmt.Errorf("sftp client pool is closed")
	}
	if p.isClosed() {
		_ = client.Close()
		p.releaseClientSlot()
		return nil, fmt.Errorf("sftp client pool is closed")
	}
	return &ClientLease{
		Client: client,
		pool:   p,
	}, nil
}

func (p *ClientPool) Close() {
	p.once.Do(func() {
		p.mu.Lock()
		p.closed = true
		close(p.done)
		p.mu.Unlock()

		for {
			select {
			case client := <-p.clients:
				_ = client.Close()
				p.releaseClientSlot()
			default:
				return
			}
		}
	})
}

func (c *ClientLease) Release() {
	c.once.Do(func() {
		c.pool.release(c.Client)
	})
}

func (c *ClientLease) WrapReadCloser(rd io.ReadCloser) *LeasedReadCloser {
	return &LeasedReadCloser{
		ReadCloser: rd,
		lease:      c,
	}
}

func (rd *LeasedReadCloser) Close() error {
	err := rd.ReadCloser.Close()
	rd.lease.Release()
	return err
}

func (p *ClientPool) release(client *sftp.Client) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		_ = client.Close()
		p.releaseClientSlot()
		return
	}

	p.clients <- client
}

func (p *ClientPool) releaseClientSlot() {
	select {
	case <-p.clientSlots:
	default:
	}
}

// Note: ClientPool is lazily initialized, Connect may be called after Lease()
// Do a shallow copy of the params map to avoid modifying the original map.
// TODO: Discuss if this is the correct approach.
func cloneParams(params map[string]string) map[string]string {
	cloned := make(map[string]string, len(params))
	for key, value := range params {
		cloned[key] = value
	}
	return cloned
}
