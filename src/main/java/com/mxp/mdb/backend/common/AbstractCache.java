package com.mxp.mdb.backend.common;

import com.mxp.mdb.common.error.Error;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author mxp
 * @date 2023/4/11 21:48
 */
public abstract class AbstractCache<T> {

    /**
     * 实际缓存的数据
     */
    private final HashMap<Long, T> cache;
    /**
     * 元素的引用个数
     */
    private final HashMap<Long, Integer> references;
    /**
     * 正在获取某资源的线程
     */
    private final Set<Long> getting;

    /**
     * 缓存的最大缓存资源数
     */
    private final int capacity;

    private int count;

    private final Lock lock;

    public AbstractCache(int capacity) {
        this.count = 0;
        this.capacity = capacity;
        cache = new HashMap<>();
        references = new HashMap<>();
        getting = new HashSet<>();
        lock = new ReentrantLock();
    }

    /**
     * 从缓存中读取值
     * @param key
     * @return
     */
    public T get(long key) throws Exception {
        while (true) {
            lock.lock();
            if (getting.contains(key)) {
                lock.unlock();
                try {
                    TimeUnit.MILLISECONDS.sleep(2);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                continue;
            }

            T it;
            if ((it = cache.get(key)) != null) {
                references.computeIfPresent(key, (k, v) -> v + 1);
                lock.unlock();
                return it;
            }

            if (capacity > 0 && count == capacity) {
                lock.unlock();
                throw Error.CacheFullException;
            }

            ++count;
            getting.add(key);
            lock.unlock();
            break;
        }

        T val;
        try {
            val = getForCache(key);
        } catch (Exception e) {
            lock.lock();
            try {
                --count;
                getting.remove(key);
            } finally {
                lock.unlock();
            }
            throw e;
        }

        lock.lock();
        try {
            cache.put(key, val);
            getting.remove(key);
            references.put(key, 1);
        } finally {
            lock.unlock();
        }
        return val;
    }

    /**
     * 强行释放一个缓存
     * @param key
     */
    public void release(long key) {
        lock.lock();
        try {
            int cnt = references.get(key);
            if (cnt == 1) {
                releaseForCache(cache.get(key));
                references.remove(key);
                cache.remove(key);
                --count;
            } else {
                references.computeIfPresent(key, (k, v) -> v - 1);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 关闭缓存时，写回所有数据
     */
    public void close() {
        lock.lock();
        try {
            Iterator<Map.Entry<Long, T>> iterator = cache.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<Long, T> entry = iterator.next();
                releaseForCache(entry.getValue());
                references.remove(entry.getKey());
                iterator.remove();
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 当资源不在缓存时的获取行为
     * @param key
     * @return
     * @throws Exception
     */
    protected abstract T getForCache(long key) throws Exception;

    /**
     * 当资源被驱逐时的写回行为
     * @param obj
     */
    protected abstract void releaseForCache(T obj);
}
