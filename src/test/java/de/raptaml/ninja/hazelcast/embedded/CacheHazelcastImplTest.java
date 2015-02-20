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

import java.util.LinkedHashMap;
import ninja.NinjaTest;
import ninja.utils.NinjaMode;
import ninja.utils.NinjaPropertiesImpl;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;



/**
 *
 * @author lemmer
 */
public class CacheHazelcastImplTest extends NinjaTest {
    
    NinjaPropertiesImpl ninjaProperties = new NinjaPropertiesImpl(NinjaMode.test);
    CacheHazelcastImpl cache;
          
    @Before
    public void init() throws Exception{
        cache = getInjector().getInstance(CacheHazelcastImpl.class);
    }
    
    @After
    public void terminate() {
        cache.destroyCacheService();
    }
   
    @Test
    public void testAdd() {
        cache.add("1", "String", 0);
        Assert.assertEquals("String", cache.get("1"));
        
        cache.add("2", 22, 0);
        Assert.assertEquals(22, cache.get("2"));
        
        cache.add("3", 3.3, 0);
        Assert.assertEquals(3.3, cache.get("3"));
        
        cache.add("4", true, 0);
        Assert.assertEquals(true, cache.get("4"));
        
    }
    
    @Test
    public void testSafeAdd() {
        Assert.assertTrue(cache.safeAdd("61", "String", 0));
        Assert.assertFalse(cache.safeAdd("61", "String2", 0));
        Assert.assertEquals("String", cache.get("61"));
        
    }
    
    @Test
    public void testSet() {
        cache.set("1", "String", 0);
        Assert.assertEquals("String", cache.get("1")); 
    }
    
    @Test
    public void testSafeSet() throws Exception {
        Assert.assertTrue(cache.safeSet("1", "String", 0));
        
        final CacheHazelcastImpl cache2 = new CacheHazelcastImpl(ninjaProperties);

        //Spawn new thread for simulating foreign lock-holder
        Thread t = new Thread() {
            @Override
            public void run() {
                cache2.lock("1", 100);
            }
        };
        t.start();

        //wait for other thread to finish so lock is taken
        synchronized(t) {
            t.wait();
            //should fail while locked by t
            Assert.assertFalse(cache.safeSet("1", "String2", 0));
        }        
        cache2.destroyCacheService();
        
        //should now work as cache2 is destroyed and therfore the lock is gone...
        Assert.assertTrue(cache.safeSet("1", "String2", 0));
        
    }
    
    @Test
    public void testReplace() {
        cache.set("1", "Value", 0);
        cache.replace("1","REPLACED",0);
        Assert.assertEquals("REPLACED", cache.get("1"));
        
    }
    
    @Test
    public void testSafeReplace() throws Exception {
        cache.set("1", "Value", 0);
        Assert.assertTrue(cache.safeReplace("1","REPLACED",0));
        Assert.assertFalse(cache.safeReplace("2","REPLACED",0));
                
        final CacheHazelcastImpl cache2 = new CacheHazelcastImpl(ninjaProperties);
        //Spawn new thread for simulating foreign lock-holder
        Thread t = new Thread() {
            @Override
            public void run() {
                cache2.lock("1", 100);
            }
        };
        t.start();
        
        synchronized(t) {
            t.wait();
            //should fail while locked by t
            Assert.assertFalse(cache.safeReplace("1", "String2", 0));
        }        
        cache2.destroyCacheService();
        Assert.assertTrue(cache.safeReplace("1","REPLACED",0));
    }
    
    @Test
    public void testGet() {
        cache.set("1", "Test", 0);
        Assert.assertNotEquals(cache.get("1"), null);
        Assert.assertEquals(cache.get("2"), null);
    }
    
    @Test
    public void testGetMultiple() {
        String[] sArr = {"1","2","3"};
        LinkedHashMap<String,Object> hMap = new LinkedHashMap<>();
        
        for (String sArr1 : sArr) {
            cache.set(sArr1, "String"+sArr1, 0);
            hMap.put(sArr1,"String"+sArr1);
        }
        
        Assert.assertEquals(cache.get(sArr),hMap);
                
    }
    
    @Rule
    public ExpectedException exception = ExpectedException.none();
    @Test
    public void testIncr() throws Exception {
        Long counter = 0L;
        Integer intCounter = 0;
        
        cache.set("counter",counter,0);
        cache.incr("counter", 1);
        
        Assert.assertEquals(1L, cache.get("counter"));
        
        cache.set("counter",counter,0);
        cache.incr("counter", -10);
        
        Assert.assertEquals(-10L, cache.get("counter"));
        
        cache.set("counter",intCounter,0);
        exception.expect(ClassCastException.class);
        exception.expectMessage("Can only increment subtype of Long");
        cache.incr("counter", 1);
        
        final CacheHazelcastImpl cache2 = new CacheHazelcastImpl(ninjaProperties);
        cache.set("counter", counter, 0);
        Thread t = new Thread() {
            @Override
            public void run() {
                cache2.lock("counter", 100);
            }
        };
        t.start();

        //wait for other thread to finish so lock is taken
        synchronized(t) {
            t.wait();
            //should fail while locked by t
            
            exception.expect(IllegalStateException.class);
            exception.expectMessage("The specified key is locked by another thread");
            cache.incr("counter", 1);
        }        
        cache2.destroyCacheService();
        
    }
    
    @Test
    public void testDecr() throws Exception {
        Long counter = 2L;
        Integer intCounter = 2;
        
        cache.set("counter",counter,0);
        cache.decr("counter", 1);
        
        Assert.assertEquals(1L, cache.get("counter"));
        
        cache.set("counter",counter,0);
        cache.decr("counter", 10);
        
        Assert.assertEquals(-8L, cache.get("counter"));
        
        cache.set("counter",intCounter,0);
        exception.expect(ClassCastException.class);
        exception.expectMessage("Can only decrement subtype of Long");
        cache.decr("counter", 1);
        
        
        final CacheHazelcastImpl cache2 = new CacheHazelcastImpl(ninjaProperties);
        cache.set("counter", counter, 0);
        Thread t = new Thread() {
            @Override
            public void run() {
                cache2.lock("counter", 100);
            }
        };
        t.start();

        //wait for other thread to finish so lock is taken
        synchronized(t) {
            t.wait();
            //should fail while locked by t
            
            exception.expect(IllegalStateException.class);
            exception.expectMessage("The specified key is locked by another thread");
            cache.decr("counter", 1);
        }        
        cache2.destroyCacheService();
        
    }
    
    @Test
    public void testClear() {
        cache.set("1", "String", 0);
        cache.clear();
        Assert.assertNull(cache.get("1"));
        
    }
    
    @Test
    public void testDelete() {
        cache.set("1", "String", 0);
        cache.delete("1");
        Assert.assertNull(cache.get("1"));
    }
    
    @Test
    public void testSafeDelete() throws Exception {
        cache.set("1", "String", 0);
        Assert.assertTrue(cache.safeDelete("1"));
        
        final CacheHazelcastImpl cache2 = new CacheHazelcastImpl(ninjaProperties);
        cache.set("1", "String", 0);
        Thread t = new Thread() {
            @Override
            public void run() {
                cache2.lock("1", 100);
            }
        };
        t.start();

        //wait for other thread to finish so lock is taken
        synchronized(t) {
            t.wait();
            //should fail while locked by t
            Assert.assertFalse(cache.safeDelete("1"));
        }        
        cache2.destroyCacheService();
        
        
        
    }
    
}
