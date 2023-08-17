package pool

import "time"

type Resource struct {
	value   interface{}
	pool    *ConnectionPool
	id      uint64
	LastUse time.Time
}

func NewResource(value interface{}, ConnectionPool *ConnectionPool, id uint64) *Resource {
	return &Resource{value: value, pool: ConnectionPool, id: id}
}

func (resource *Resource) Id() uint64 {
	return resource.id
}

func (resource *Resource) Value() interface{} {
	return resource.value
}

func (resource *Resource) Release() {
	resource.pool.Release(resource)
}

func (resource *Resource) Destroy() {
	resource.pool.Destroy(resource)
}
