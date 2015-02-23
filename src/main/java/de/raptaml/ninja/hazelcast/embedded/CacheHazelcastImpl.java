/*
 * Copyright 2015 Matthias Lemmer.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package de.raptaml.ninja.hazelcast.embedded;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.hazelcast.config.Config;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.InterfacesConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import ninja.cache.Cache;
import ninja.lifecycle.Dispose;
import ninja.utils.NinjaProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author lemmer
 */
@Singleton
public class CacheHazelcastImpl implements Cache {
    private static final Logger LOG = LoggerFactory.getLogger(CacheHazelcastImpl.class);
    private final NinjaProperties ninjaProperties;
    
    private final String bindAdress;
    private final int bindPort;
    private final String groupName;
    private final String groupSecret;
    
    private final HazelcastInstance instance;
    private final Config config;
    private final NetworkConfig network;
    private final GroupConfig group;
    
    private final IMap<String, Object> cache;
    
      
    
    
    @Inject
    public CacheHazelcastImpl(NinjaProperties ninjaProperties) throws Exception {
        this.ninjaProperties = ninjaProperties;
        this.bindAdress = ninjaProperties.getOrDie("ninja.hazelcast.interface_ip");
        this.bindPort = ninjaProperties.getIntegerOrDie("ninja.hazelcast.outbound_port");
        this.groupName = ninjaProperties.getOrDie("ninja.hazelcast.groupname");
        this.groupSecret = ninjaProperties.getOrDie("ninja.hazelcast.groupsecret");
                                
        this.config = new Config();
        network = new NetworkConfig().setInterfaces(new InterfacesConfig().clear()
                                                                          .addInterface(bindAdress)
                                                                          .setEnabled(true))
                                     .setPort(bindPort);
        
        group = new GroupConfig(groupName, groupSecret);
        
        config.setNetworkConfig(network);
        config.setGroupConfig(group);
        
        this.instance = Hazelcast.newHazelcastInstance(config);
        
        cache = instance.getMap("cache");            
    }
    
    

    @Override
    public void add(String key, Object value, int expiration) {
        cache.putIfAbsent(key, value, expiration, TimeUnit.SECONDS);
    }

    @Override
    public boolean safeAdd(String key, Object value, int expiration) {
        
        try {
            if (!cache.tryLock(key, 2, TimeUnit.SECONDS)) {
                return false;
            }
            return cache.putIfAbsent(key, value, expiration, TimeUnit.SECONDS) == null ;
        } catch (InterruptedException ex) {
            return false;
        } finally {
            cache.unlock(key);
        }
    }

    @Override
    public void set(String key, Object value, int expiration) {
        cache.set(key, value, expiration, TimeUnit.SECONDS);
    }

    @Override
    public boolean safeSet(String key, Object value, int expiration) {
        
        try {
            if (!cache.tryLock(key, 2, TimeUnit.SECONDS)) {
            return false;
            }
            try {
                cache.set(key, value, expiration, TimeUnit.SECONDS);
                return cache.get(key).equals(value);
            } finally {
                cache.unlock(key);
            }
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public void replace(String key, Object value, int expiration) {
        if (cache.containsKey(key)) {
            cache.set(key, value,expiration, TimeUnit.SECONDS);
        }
    }
     
    @Override
    public boolean safeReplace(String key, Object value, int expiration) {
        try {
            if (!cache.tryLock(key, 2, TimeUnit.SECONDS)) {
            return false;
            }
            try {
                Object oldValue = cache.get(key);
                if (cache.containsKey(key) && cache.get(key).equals(oldValue)) {
                    cache.set(key, value, expiration, TimeUnit.SECONDS);
                    return true;
                }
                return false;
            } finally {
                cache.unlock(key);
            }
        }catch (Exception e) {
            return false;
        }
    }

    @Override
    public Object get(String key) {
        return cache.get(key);
    }

    @Override
    public Map<String, Object> get(String[] keys) {
        return cache.getAll(new LinkedHashSet<>(Arrays.asList(keys)));
    }

    @Override
    public long incr(String key, int by) {
        if (!cache.tryLock(key)) {
            throw new IllegalStateException("The specified key is locked by another thread");
        }
        try {
            //this will throw if cached Object is not a Long
            cache.get(key).getClass().asSubclass(Long.class);
            long num =(long) cache.get(key);
            num = num + by;
            return (long)cache.put(key, (Long)num);
        }catch (ClassCastException e){
            throw new ClassCastException("Can only increment subtype of Long");
        //prevent dead locks at any cost    
        } finally {
            cache.unlock(key);
        }
    }
    
    @Override
    public long decr(String key, int by) {
        if (!cache.tryLock(key)) {
            throw new IllegalStateException("The specified key is locked by another thread");
        }
        try {
            //this will throw if cached Object is not a Long
            cache.get(key).getClass().asSubclass(Long.class);
            long num =(long) cache.get(key);
            num = num - by;
            return (long)cache.put(key, (Long)num);
        }catch (ClassCastException e){
            throw new ClassCastException("Can only decrement subtype of Long");
        //prevent dead locks at any cost    
        } finally {
            cache.unlock(key);
        }
    }

    @Override
    public void clear() {
        cache.clear();
    }

    @Override
    public void delete(String key) {
        cache.delete(key);
    }

    @Override
    public boolean safeDelete(String key) {
        return cache.tryRemove(key, 2, TimeUnit.SECONDS);
    }
    
    @Dispose
    public void stop() {
        if (instance != null) {
            this.instance.shutdown();
        }
        
    }
    
     //Methods for testing purposes. Not exposed via Interface
    public void destroyCacheService() {
        this.instance.shutdown();
    }
    public void lock(String key, long seconds) {
        cache.lock(key,seconds,TimeUnit.SECONDS);
    }
    public void unlock(String key) {
        cache.unlock(key);
    }
    
    
}
