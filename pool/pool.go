package pool

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/tangnguyendeveloper/ConnectionPool/queue"
)

type Config struct {
	// Max connection of the pool
	MaxResource uint64
	// Min idle connection of the pool
	MinIdleResource uint64

	// Function to Open connection
	Constructor func(context.Context) (net.Conn, error)
	// Function to Close connection
	Destructor func(value net.Conn)

	// Time interval to reconnect if the connection of the pool are lost
	// (in seconds)
	ReconnectInterval int64
	// The time duration to remove the connection of the pool if that connection is not use
	// (in seconds)
	IdleKeepAlive float64
}

// TCP Connection Pool
type ConnectionPool struct {
	mux    sync.Mutex
	config *Config

	ctx context.Context

	closed bool

	idleResource   *queue.Queue
	activeResource []*Resource
}

// New TCP Connection Pool
func NewConnectionPool(config *Config) *ConnectionPool {
	return &ConnectionPool{
		config:       config,
		idleResource: &queue.Queue{MaxSize: config.MaxResource},
		closed:       true,
	}
}

// All resource
func (p *ConnectionPool) NumResource() uint64 {
	return p.NumActiveResource() + p.NumIdleResource()
}

// The resource are idle
func (p *ConnectionPool) NumIdleResource() uint64 {
	return uint64(p.idleResource.Length())
}

// The resource are acquired
func (p *ConnectionPool) NumActiveResource() uint64 {
	return uint64(len(p.activeResource))
}

func (p *ConnectionPool) Config() *Config {
	return p.config
}

// Startup the ConnectionPool.
// Please call with a Goroutine
func (p *ConnectionPool) Start(ctx context.Context) error {

	p.ctx = ctx

	p.closed = false

	for !p.closed {

		select {
		case <-p.ctx.Done():
			p.closed = true
			p.reset()
			return p.ctx.Err()
		default:
		}

		if p.config.MinIdleResource > p.NumResource() {
			p.initConnection(p.config.MinIdleResource - p.NumResource())
		}

		if p.config.MinIdleResource < p.NumIdleResource() {
			p.cleanupIdle()
		}

		p.cleanupLost()

		time.Sleep(time.Second * time.Duration(p.config.ReconnectInterval))
	}

	return nil
}

func (p *ConnectionPool) initConnection(num_connection uint64) {
	for num_connection > 0 {
		p.newIdle()
		num_connection--
	}
}

func (p *ConnectionPool) cleanupLost() {
	p.mux.Lock()

	if p.idleResource.Peek(0) == nil {
		p.mux.Unlock()
		return
	}

	one := make([]byte, 1)

	resource := p.idleResource.Peek(0).(*Resource)
	if resource == nil {
		p.mux.Unlock()
		return
	}

	resource.Value().(*net.TCPConn).SetReadDeadline(time.Now().Add(time.Second))
	_, err := resource.Value().(*net.TCPConn).Read(one)
	if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
		p.mux.Unlock()
		return
	} else if err != nil {
		p.reset()
	}

	p.mux.Unlock()

}

func (p *ConnectionPool) reset() {
	for resource := p.idleResource.Dequeue(); resource != nil; resource = p.idleResource.Dequeue() {
		p.config.Destructor(resource.(*Resource).Value().(net.Conn))
	}

	for _, resource := range p.activeResource {
		p.config.Destructor(resource.Value().(net.Conn))
	}

	p.activeResource = p.activeResource[:0]
}

func (p *ConnectionPool) cleanupIdle() {
	p.mux.Lock()

	n := p.NumIdleResource()
	rm := n - p.config.MinIdleResource
	k := uint64(0)

	for n > 0 {

		resource := p.idleResource.Peek(n - 1).(*Resource)

		if time.Since(resource.LastUse).Seconds() > p.config.IdleKeepAlive {
			p.config.Destructor(resource.Value().(net.Conn))

			p.idleResource.Remove(n - 1)

			k++
			if k == rm {
				p.mux.Unlock()
				return
			}
		}
		n--
	}
	p.mux.Unlock()
}

func (p *ConnectionPool) newIdle() error {

	resource_value, err := p.config.Constructor(p.ctx)
	if err != nil {
		return err
	}

	resource := NewResource(resource_value, p, p.NumResource())
	resource.LastUse = time.Now()

	p.mux.Lock()
	err = p.idleResource.Enqueue(resource)
	p.mux.Unlock()

	if err != nil {
		return err
	}
	return nil
}

func (p *ConnectionPool) Acquire(ctx context.Context) (*Resource, error) {
	if p.closed {
		return nil, fmt.Errorf("Acquire ERROR: The ConnectionPool was closed")
	}

	var resource *Resource
	var err bool

	for !p.closed {
		select {
		case <-p.ctx.Done():
			p.closed = true
			p.reset()
			return nil, p.ctx.Err()
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		resource, err = p.acquire()
		if err {
			time.Sleep(time.Millisecond)
			continue
		} else {
			break
		}
	}

	return resource, nil
}

func (p *ConnectionPool) acquire() (*Resource, bool) {

	p.mux.Lock()
	resource := p.idleResource.Dequeue()
	if resource != nil {
		resource.(*Resource).Value().(*net.TCPConn).SetDeadline(time.Time{})
		p.activeResource = append(p.activeResource, resource.(*Resource))
		p.mux.Unlock()
		return resource.(*Resource), true
	}

	if p.NumResource() == p.config.MaxResource {
		p.mux.Unlock()
		return nil, false
	}

	resource_value, err := p.config.Constructor(p.ctx)
	if err != nil {
		p.mux.Unlock()
		return nil, false
	}

	resource = NewResource(resource_value, p, p.NumResource())
	p.activeResource = append(p.activeResource, resource.(*Resource))
	p.mux.Unlock()

	return resource.(*Resource), true
}

func (p *ConnectionPool) removeFromActiveResourceWithID(id uint64) bool {
	p.mux.Lock()

	for idx, resource := range p.activeResource {
		if resource.Id() == id {
			p.activeResource = append(p.activeResource[:idx], p.activeResource[idx+1:]...)
			p.mux.Unlock()
			return true
		}
	}

	p.mux.Unlock()
	return false
}

func (p *ConnectionPool) Release(resource *Resource) {

	ok := p.removeFromActiveResourceWithID(resource.Id())
	if !ok {
		return
	}

	resource.LastUse = time.Now()

	p.mux.Lock()
	defer p.mux.Unlock()

	err := p.idleResource.Enqueue(resource)
	if err != nil {
		p.config.Destructor(resource.Value().(net.Conn))
	}
}

func (p *ConnectionPool) Destroy(resource *Resource) {
	ok := p.removeFromActiveResourceWithID(resource.Id())
	if !ok {
		return
	}
	p.config.Destructor(resource.Value().(net.Conn))
}

func (p *ConnectionPool) Close() {

	p.mux.Lock()
	defer p.mux.Unlock()

	p.closed = true

	p.reset()
}

func (p *ConnectionPool) SendSingle(payload []byte) error {
	if p.closed {
		return fmt.Errorf("SendSingle ERROR: the ConnectionPool is not being startup or closed")
	}

	select {
	case <-p.ctx.Done():
		p.closed = true
		p.reset()
		return p.ctx.Err()
	default:
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)

	resource, err := p.Acquire(ctx)
	if err != nil {
		cancel()
		return err
	}

	_, err = resource.Value().(*net.TCPConn).Write(payload)
	if err != nil {
		resource.Destroy()
	} else {
		resource.Release()
	}

	cancel()
	return err
}

// Send request and return the Resource connection to receive response.
//
//	the Resource connection must be release.
func (p *ConnectionPool) SendRequest(request []byte) (*Resource, error) {
	if p.closed {
		return nil, fmt.Errorf("SendRequest ERROR: the ConnectionPool is not being startup or closed")
	}

	select {
	case <-p.ctx.Done():
		p.closed = true
		p.reset()
		return nil, p.ctx.Err()
	default:
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)

	resource, err := p.Acquire(ctx)
	if err != nil {
		cancel()
		return nil, err
	}

	_, err = resource.Value().(*net.TCPConn).Write(request)
	if err != nil {
		resource.Destroy()
		cancel()
		return nil, err
	}

	cancel()
	return resource, err
}

func (p *ConnectionPool) SendMultiple(payloads [][]byte) error {
	if p.closed {
		return fmt.Errorf("SendMultiple ERROR: the ConnectionPool is not being startup or closed")
	}

	select {
	case <-p.ctx.Done():
		p.closed = true
		p.reset()
		return p.ctx.Err()
	default:
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)

	resource, err := p.Acquire(ctx)
	if err != nil {
		cancel()
		return err
	}

	for _, payload := range payloads {
		_, err = resource.Value().(*net.TCPConn).Write(payload)
		if err != nil {
			resource.Destroy()
		} else {
			resource.Release()
		}
	}

	cancel()
	return err
}
