package com.cachakka.streaming.akka.shard

import com.cachakka.streaming.akka.shard.cache.CacheRegistryStatusEnum.{REGISTERING, SUBSCRIBED, UNREGISTERING, UNSUBSCRIBED}
import com.cachakka.streaming.akka.shard.cdc.{CdcProcessingUpdate, DbUpsert}
import com.cachakka.streaming.akka.{AkkaCluster, ShardedEntityOperationProvider}

package object cache {

  val PRIMARY_CACHE_INDEX_ID = "primary"

  val CACHE_SHARD_ID_SIGNATURE: String = "Cache"

  val CACHE_STATUS_REGISTERING: CacheStatusRegistering = REGISTERING
  val CACHE_STATUS_UNREGISTERING: CacheStatusUnregistering  = UNREGISTERING
  val CACHE_STATUS_SUBSCRIBED: CacheStatusSubscribed = SUBSCRIBED
  val CACHE_STATUS_UNSUBSCRIBED: CacheStatusUnsubscribed  = UNSUBSCRIBED

  val GROUP_ID_KEY = "group"

  val QUANTITY_KEY = "quantity"

  val TIMESTAMP_KEY = "timestamp"

  val INCLUDE_FILTER_KEY = "include"

  val EXCLUDE_FILTER_KEY = "exclude"

  val QUERY_LIMIT = "query_limit"

  val AND_PREDICATE = "and_predicate"

  val OR_PREDICATE = "or_predicate"

  val DIGEST_FIELD = "digest_field"

  lazy val DbUpsert: DbUpsert = AkkaCluster.injector.getInstance(classOf[ShardedEntityOperationProvider]).findInterfaceOperation(classOf[DbUpsert]).get
  lazy val CdcProcessingUpdate: CdcProcessingUpdate = AkkaCluster.injector.getInstance(classOf[ShardedEntityOperationProvider]).findInterfaceOperation(classOf[CdcProcessingUpdate]).get
  lazy val CacheUpdate: CacheUpdate = AkkaCluster.injector.getInstance(classOf[ShardedEntityOperationProvider]).findInterfaceOperation(classOf[CacheUpdate]).get
  lazy val CacheGroupStart: CacheGroupStart = AkkaCluster.injector.getInstance(classOf[ShardedEntityOperationProvider]).findInterfaceOperation(classOf[CacheGroupStart]).get
  lazy val CacheGroupInitialize: CacheGroupInitialize = AkkaCluster.injector.getInstance(classOf[ShardedEntityOperationProvider]).findInterfaceOperation(classOf[CacheGroupInitialize]).get
  lazy val CacheGroupStop: CacheGroupStop = AkkaCluster.injector.getInstance(classOf[ShardedEntityOperationProvider]).findInterfaceOperation(classOf[CacheGroupStop]).get
  lazy val CacheGroupPoll: CacheGroupPoll = AkkaCluster.injector.getInstance(classOf[ShardedEntityOperationProvider]).findInterfaceOperation(classOf[CacheGroupPoll]).get


}
