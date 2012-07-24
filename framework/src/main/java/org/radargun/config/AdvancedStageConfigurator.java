package org.radargun.config;

import org.radargun.Stage;
import org.w3c.dom.Element;

/**
 * Configures Stage object from it's XML element. Stage sub-classes that want to be configured with
 * {@code AdvancedStageConfigurator} need to create a static inner class implementing it.
 * 
 * @author Michal Linhard <mlinhard@redhat.com>
 */
public interface AdvancedStageConfigurator {
   /**
    * 
    * Configures Stage object from it's XML element.
    * 
    * @param stageElement
    * @param stage
    * @throws Exception
    */
   void configure(Element stageElement, Stage stage) throws Exception;
}
