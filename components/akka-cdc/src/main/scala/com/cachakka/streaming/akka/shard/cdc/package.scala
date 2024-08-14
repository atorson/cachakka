package com.cachakka.streaming.akka.shard

import java.util.concurrent.atomic.{AtomicInteger, AtomicLong, AtomicReference}

import com.cachakka.streaming.akka.shard.cdc.CDCShardExtension.CDC

package object cdc {

  type CDCProcessingInfo[E<:CDC[E]] = (DelayedCDCProcessingCompanion[E], AtomicLong, AtomicInteger, scala.collection.concurrent.Map[String, AtomicInteger])

  type CDCShardInfo[E<:CDC[E]] = (CDCEntityCompanion[E], AtomicReference[(Int, AtomicInteger)], Option[CDCProcessingInfo[E]])

  val DELTA_BUCKET_SIZE = 2500

  val DELTA_MAX_BUCKET_NUMBER = 1000

  val CDC_PROCESSING_BUCKET_INTERVAL_MILLIS = 1000*1000

  val CDC_PROCESSING_MAX_BUCKET_NUMBER = 1000

}
