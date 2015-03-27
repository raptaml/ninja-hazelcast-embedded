[![Maven Central](https://maven-badges.herokuapp.com/maven-central/de.raptaml/ninja-hazelcast-embedded/badge.svg)](https://maven-badges.herokuapp.com/maven-central/de.raptaml/ninja-hazelcast-embedded)

# ninja-hazelcast-embedded

This is an implementation of Ninja Frameworks Cache interface for [hazelcast][2].  
Its structure is somehow based on [svenkubiaks][1] excellent MongoDB module and therefore it is an easys plugable jar, which you just have to add to your Ninja-powered projects dependencies.
## Purpose
Ninja allready gives you two fine implementations for ehcache and memcached client.  
But in some use cases you might consider the following setup for your horizontally scalable app:  
![alt tag](https://github.com/raptaml/ninja-hazelcast-embedded/blob/master/hazelcast.png)

This plugin gives you the possibility to start a hazelcast instance with every Ninja app you deploy and let them automatically join to a cluster (first node starts the cluster).
## Configuration
To use it, just add this to your application.conf:
```
cache.implementation=de.raptaml.ninja.hazelcast.embedded.CacheHazelcastImpl
```
Specify the interface and port on which hazelcast should be listening for incoming clustering requests:
```
ninja.hazelcast.interface_ip=127.0.0.1
ninja.hazelcast.outbound_port=5701
```
So you get a fail-safe, reliable cache which grows with every Ninja instance serving your application.  
You dont want wild spreading cache cluster? No problem. Just configure the group settings in your application.conf like so:  
```
ninja.hazelcast.groupname=group1
ninja.hazelcast.groupsecret=toBeChanged
```
Now you can separate the caches from each other even if you are running them in the same address range.  

**!! Please note !!**  
You should really not run your cache on interfaces/addresses which are publicly available. Even if you have a group config in place, which is mandatory for this plugin, you should run your cache on interfcaces connected to a separate and firewalled subnet.
Unfortunately hazelcast supports TLS and fine granular access control only in enterprise version at the moment.
Keep that in mind when designing your application.







[1]: https://github.com/svenkubiak/ninja-mongodb
[2]: http://www.hazelcast.org/
[3]: https://github.com/mongodb/morphia/wiki/GettingStarted
