package org.radargun;

/**
 * 
 * Additional CacheWrapper feature, allows simulation of abrupt cache shutdown (node crash).
 * 
 * @author Michal Linhard <mlinhard@redhat.com>
 */
public interface Killable {
   void kill() throws Exception;
}
