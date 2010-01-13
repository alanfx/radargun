package org.cachebench.cachewrappers;

import org.cachebench.CacheWrapper;
import org.infinispan.Cache;
import org.infinispan.remoting.transport.Address;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.context.Flag;
import org.infinispan.manager.CacheManager;
import org.infinispan.manager.DefaultCacheManager;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;

import javax.transaction.TransactionManager;
import java.util.List;

public class InfinispanWrapper implements CacheWrapper {

   private static Log log = LogFactory.getLog(InfinispanWrapper.class);
   CacheManager cacheManager;
   Cache<Object, Object> cache;
   TransactionManager tm;
   boolean started = false;
   String config;

   public void init(String config) throws Exception {
      this.config = config;                                                                                                         
      setUp();
   }

   public void setUp() throws Exception {
      if (!started) {
         cacheManager = new DefaultCacheManager(config);
         // use the default cache
         cache = cacheManager.getCache();
         started = true;
      }
      log.info("Loading jgroups form: " + org.jgroups.Version.class.getProtectionDomain().getCodeSource().getLocation());
      log.info("JGroups version: " + org.jgroups.Version.description);//2.8.
      log.info("JGroups version: " + org.jgroups.Version.major); //2
      log.info("JGroups version: " + org.jgroups.Version.minor);//8
      log.info("JGroups version: " + org.jgroups.Version.micro);//0
      log.info("JGroups version: " + org.jgroups.Version.printDescription());
   }

   public void tearDown() throws Exception {
      List<Address> addressList = cacheManager.getMembers();
      if (started) {
         cacheManager.stop();
         log.trace("Stopped, previous view is " + addressList);
         started = false;
      }
   }

   public void put(String bucket, Object key, Object value) throws Exception {
      cache.put(bucket + key, value);
   }

   public Object get(String bucket, Object key) throws Exception {
      return cache.get(bucket + key);
   }

   public void empty() throws Exception {
      log.info("Cache size before clear: " + cache.size());
      cache.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL).clear();
      log.info("Cache size after clear: " + cache.size());
   }

   public int getNumMembers()  {
      ComponentRegistry componentRegistry = cache.getAdvancedCache().getComponentRegistry();
      if (componentRegistry.getStatus().startingUp()) {
         log.trace("We're in the process of starting up.");
      }
      if (cacheManager.getMembers() != null) {
         log.trace("Members are: " + cacheManager.getMembers());
      }
      return cacheManager.getMembers() == null ? 0 : cacheManager.getMembers().size();
   }

   public String getInfo() {
      return cache.getVersion() + ", " + config + ", Size of the cache is: " + cache.size();
   }

   public Object getReplicatedData(String bucket, String key) throws Exception {
      return get(bucket, key);
   }

   public Object startTransaction() {
      if (tm == null) return null;
      try {
         tm.begin();
         return tm.getTransaction();
      }
      catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   public void endTransaction(boolean successful) {
      if (tm == null) return;
      try {
         if (successful)
            tm.commit();
         else
            tm.rollback();
      }
      catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   public static void main(String[] args) throws Exception {
      InfinispanWrapper wrapper = new InfinispanWrapper();
      wrapper.init("dist-async.xml");
      wrapper.setUp();
   }
}