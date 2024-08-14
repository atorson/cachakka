package com.cachakka.streaming.akka;

import akka.actor.ActorSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

/**
 * This bootable app is only used when Akka co-lo is deployed outside of Flink JVM
 */
public class AkkaStandaloneBoot {

    private static final Logger logger = LoggerFactory.getLogger(AkkaStandaloneBoot.class);

    public static void main(String[] args) throws Exception {
        ActorSystem system = AkkaCluster.actorSystem();
        logger.info("Akka co-lo cluster system: " + system.toString() + " is running");
        Await.result(system.whenTerminated(), Duration.Inf());
    }
}
