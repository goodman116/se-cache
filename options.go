package cache

import (
	"time"
)

// Options outlines the customizable fluent builder rules for configuration management.
type Options[V any] interface {
	WithMaxKeys(maxKeys int) Cache[V]
	WithDefaultTTL(ttl time.Duration) Cache[V]
	WithClockInterval(interval time.Duration) Cache[V]
	WithCleanupInterval(interval time.Duration) Cache[V]
	WithHasher(fn HasherFn) Cache[V]
	WithOnEvicted(fn func(key Key, value V)) Cache[V]
	Start() Cache[V]
}

func (c *cacheImpl[V]) WithMaxKeys(maxKeys int) Cache[V] {
	c.maxSize = maxKeys
	return c
}

func (c *cacheImpl[V]) WithDefaultTTL(ttl time.Duration) Cache[V] {
	c.defaultTTL = ttl
	return c
}

func (c *cacheImpl[V]) WithClockInterval(interval time.Duration) Cache[V] {
	c.clockInterval = interval
	return c
}

func (c *cacheImpl[V]) WithCleanupInterval(interval time.Duration) Cache[V] {
	c.cleanupInterval = interval
	return c
}

func (c *cacheImpl[V]) WithHasher(fn HasherFn) Cache[V] {
	if fn != nil {
		c.hasherFn = fn
	}
	return c
}

func (c *cacheImpl[V]) WithOnEvicted(fn func(key Key, value V)) Cache[V] {
	c.onEvicted = fn
	return c
}

func (c *cacheImpl[V]) Start() Cache[V] {
	if c.defaultTTL != noEvictionTTL && c.cleanupInterval == defaultCleanupInterval {
		calculatedInterval := c.defaultTTL / 2
		if calculatedInterval > 1*time.Minute {
			c.cleanupInterval = calculatedInterval
		}
	}

	if c.maxSize > 0 {
		perShardCap := max(1, c.maxSize/shardCount)
		for _, sh := range c.shards {
			sh.mu.Lock()
			sh.items = make(map[Key]item[V], perShardCap)
			sh.mu.Unlock()
		}
	}

	if c.clockInterval > 0 {
		c.wg.Add(1)
		go c.clockLoop(c.clockInterval)
	}

	if c.cleanupInterval > 0 && (c.maxSize > 0 || c.defaultTTL != noEvictionTTL) {
		c.wg.Add(1)
		go c.cleanupLoop(c.cleanupInterval)
	}

	return c
}
