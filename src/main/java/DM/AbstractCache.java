package DM;

import common.Errors;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public abstract class AbstractCache<T> {

    // TODO: why the passed-down resource identifier is long?
    private HashMap<Long, T> cache; // actual cached resource
    private HashMap<Long, Integer> references; // number of times this resource is referring to
    private HashMap<Long, Boolean> getting; // resources that is being retrieved from database

    private int maxResource;                            // max number of resources in cache
    private int count = 0;                              // actual number of resources in cache
    private Lock lock;

    public AbstractCache(int maxResource) {
        this.maxResource = maxResource;
        this.lock = new ReentrantLock();
        this.cache = new HashMap<>();
        this.references = new HashMap<>();
        this.getting = new HashMap<>();
    }

    /**
     * 1. If other threads are getting the resources, wait for them (so that in later cache, it is the updated resource)
     * 2. Find the resource in cache
     * 3. If no thread is getting this resource, getForCache()
     *
     * @param key - resource identifier
     * @return
     */
    protected T get(long key) throws Exception {
        // search in getting & waiting for possible updated resource
        while (true) {
            lock.lock();
            if (getting.containsKey(key)) {
                lock.unlock();
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    continue;
                }
                continue;
            }

            // search in cache
            if (cache.containsKey(key)) {
                T obj = cache.get(key);
                references.put(key, references.get(key) + 1);
                lock.unlock();
                return obj;
            }

            // if cache is full, throw exception
            if (maxResource > 0 && count == maxResource) {
                lock.unlock();
                throw Errors.CacheFullException;
            }

            // mark: trying to get the resource
            getting.put(key, true);
            lock.unlock();
            break;
        }

        // get the resource
        T obj = null;
        try {
            obj = getForCache(key);
        } catch (Exception e) {
            lock.lock();
            getting.remove(key);
            lock.unlock();
            throw e;
        }

        lock.lock();
        count++;
        getting.remove(key);
        cache.put(key, obj);
        references.put(key, 1);
        lock.unlock();

        return obj;
    }

    /**
     * Decrement the references of the resource by 1
     * If the references reach 0, we need to release this resource from cache
     * Otherwise, update the references
     * @param key
     */
    protected void release(long key) {
        lock.lock();
        try {
            int ref = references.get(key) - 1;
            if (ref == 0) {
                // remove the resource from
                releaseForCache(cache.get(key));
                cache.remove(key);
                references.remove(key);
                count--;
            } else {
                references.put(key, ref);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * When the cache closes, clean the cache and release all resources
     */
    protected void close() {
        lock.lock();
        try {
            Set<Long> keySet = cache.keySet();
            for (long key: keySet) {
                release(key);
                references.remove(key);
                cache.remove(key);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Retrieve the data from database/source
     * @param key
     * @return
     * @throws Exception
     */
    protected abstract T getForCache(long key) throws Exception;

    /**
     * Write back to the database/source
     * @param object
     */
    protected abstract void releaseForCache(T object);
}
