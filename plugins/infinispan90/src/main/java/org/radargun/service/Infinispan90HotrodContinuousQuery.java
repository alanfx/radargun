package org.radargun.service;

import org.infinispan.client.hotrod.event.ClientEvents;
import org.radargun.traits.ContinuousQuery;
import org.radargun.traits.Query;

/**
 * @author Vojtech Juranek &lt;vjuranek@redhat.com&gt;
 */
public class Infinispan90HotrodContinuousQuery extends InfinispanHotrodContinuousQuery {

   protected final InfinispanHotrodService service;

   public Infinispan90HotrodContinuousQuery(InfinispanHotrodService service) {
      super(service);
      this.service = service;
   }

   @Override
   public Infinispan90HotrodContinuousQuery.ListenerReference createContinuousQuery(String cacheName, Query query, ContinuousQuery.Listener cqListener) {
      AbstractInfinispanQueryable.QueryImpl ispnQuery = (AbstractInfinispanQueryable.QueryImpl) query;
      Infinispan90HotrodContinuousQuery.Listener ispnCqListener = new Infinispan90HotrodContinuousQuery.Listener(cqListener);
      ClientEvents.addClientQueryListener(getRemoteCache(cacheName), ispnCqListener, ispnQuery.getDelegatingQuery());

      org.infinispan.client.hotrod.Search.getContinuousQuery(getRemoteCache(cacheName)).addContinuousQueryListener(ispnQuery.getDelegatingQuery(), ispnCqListener);
      return new Infinispan90HotrodContinuousQuery.ListenerReference(ispnCqListener);
   }

   @Override
   public void removeContinuousQuery(String cacheName, ContinuousQuery.ListenerReference listenerReference) {
      ListenerReference ref = (Infinispan90HotrodContinuousQuery.ListenerReference) listenerReference;
      getRemoteCache(cacheName).removeClientListener(ref.clientListener);
   }

   private static class Listener implements org.infinispan.query.api.continuous.ContinuousQueryListener {

      private final ContinuousQuery.Listener cqListener;

      public Listener(ContinuousQuery.Listener cqListener) {
         this.cqListener = cqListener;
      }

      @Override
      public void resultJoining(Object key, Object value) {
         cqListener.onEntryJoined(key, value);
      }

      @Override
      public void resultLeaving(Object key) {
         cqListener.onEntryLeft(key);
      }
   }
}
