package com.cachakka.streaming.akka.shard.cdc

import java.nio.ByteBuffer
import java.time.Instant
import java.util.{Date, UUID}

import com.datastax.driver.core._
import com.google.protobuf.ByteString
import com.google.protobuf.timestamp.Timestamp
import com.typesafe.scalalogging.LazyLogging
import com.cachakka.streaming.akka.findCompanion
import com.cachakka.streaming.configuration.ConfigurationProvider
import com.cachakka.streaming.connectors.{CassandraClusterConnector, CassandraClusterConnectorImpl}
import com.cachakka.streaming.extensions.CQLBatchAwareAsyncContext
import io.getquill.context.cassandra.Udt
import io.getquill.context.cassandra.encoding.{CassandraMapper, CassandraType}

import scala.collection.JavaConverters
import scala.concurrent.ExecutionContext
import scala.util.Try

object ScalaCassTypeMapper {

  def apply[J,S](implicit t: CassandraType[J]): ScalaCassTypeMapper[J,S] = new ScalaCassTypeMapper[J,S] {override val cassType = t}

}

trait ScalaCassTypeMapper[J,S]{
  val cassType: CassandraType[J]
}

/**
  * Customized Quill-IO Cassandra context providing extra Encoders/Decoders for complex nested collections use-cases and some generic Proto types SerDe
  * DON'T MODIFY THIS CODE UNLESS YOU"RE a QUILL-IO EXPERT. THIS CODE PLAYS ALONG WITH THE QUILL-IO Scala Macros which are very powerful but super-hard to understand and debug
  * @param cluster
  * @param keyspace
  * @param preparedStatementCacheSize
  * @param enums
  * @param ec
  */
class CDCCassandraContext(val cluster: Cluster, val keyspace: String, val preparedStatementCacheSize: Long, val enums: Map[String, CDCEnum])(override implicit val ec: ExecutionContext)
  extends CQLBatchAwareAsyncContext(cluster, keyspace, preparedStatementCacheSize) with LazyLogging{

  val rootUUID = UUID.fromString("d4d1c83d-d63a-46cf-8597-d6e8d2a0b0ba")

  implicit val dateTypeMapper = ScalaCassTypeMapper[Date, Timestamp]
  implicit val boolTypeMapper = ScalaCassTypeMapper[java.lang.Boolean, Boolean]
  implicit val intTypeMapper = ScalaCassTypeMapper[java.lang.Integer, Int]
  implicit val shortTypeMapper = ScalaCassTypeMapper[java.lang.Short, Short]
  implicit val decimalTypeMapper = ScalaCassTypeMapper[java.math.BigDecimal, BigDecimal]
  implicit val longTypeMapper = ScalaCassTypeMapper[java.lang.Long, Long]
  implicit val doubleTypeMapper = ScalaCassTypeMapper[java.lang.Double, Double]
  implicit val floatTypeMapper = ScalaCassTypeMapper[java.lang.Float, Float]
  implicit val stringTypeMapper = ScalaCassTypeMapper[java.lang.String, String]
  implicit val uuidTypeMapper = ScalaCassTypeMapper[UUID, String]
  implicit val byteTypeMapper = ScalaCassTypeMapper[java.lang.Byte, Byte]
  implicit val byteBufferArrayTypeMapper = ScalaCassTypeMapper[ByteBuffer, Array[Byte]]
  implicit val byteBufferOptionArrayTypeMapper = ScalaCassTypeMapper[ByteBuffer, Option[Array[Byte]]]
  implicit val byteBufferStringTypeMapper = ScalaCassTypeMapper[ByteBuffer, ByteString]


  def this(connector: CassandraClusterConnector, configurationProvider: ConfigurationProvider)(implicit ec: ExecutionContext) = this(connector.getCluster,
    connector.getPrimaryKeyspaceName, connector.getPreparedCacheSize,
    Try{JavaConverters.asScalaIteratorConverter(configurationProvider.getConfig.getConfigList("cdc.entities").iterator()).asScala.toSeq}.getOrElse(Seq())
      .groupBy(x => x.getString("fqn")).keySet.foldLeft[Set[CDCEnum]](Set())((x, s) => x ++ findCompanion[CDCEnumsProvider](s, Thread.currentThread().getContextClassLoader).get.enums)
    .map(e => (e.name -> e)).toMap)

  def this(configProvider: ConfigurationProvider)(implicit ec: ExecutionContext) = this(CassandraClusterConnectorImpl.getInstance(configProvider), configProvider)

  override implicit def optionDecoder[T](implicit d: Decoder[T]): Decoder[Option[T]] =
    CassandraDecoder((index, row) =>
      row.isNull(index) match {
        case true  => None
        case false => Some(d(index, new OverridenRow(row)))
      }
    )

  override def udtValueOf(udtName: String, keyspace: Option[String]): UDTValue = new OverridenUdtValue(super.udtValueOf(udtName, keyspace))

  implicit def udtOptionDecodingMapper[V<:Udt](implicit d: CassandraMapper[UDTValue,V]): CassandraMapper[UDTValue,Option[V]] =
    CassandraMapper((i:UDTValue) => i match {
      case null => None
      case x => Some(d.f(new OverridenUdtValue(x)))
    })

  implicit def cassWrapperOptionDecodingMapper[J, S](implicit sc: ScalaCassTypeMapper[J,S], d: CassandraMapper[J,S]): MappedEncoding[J,Option[S]] =
    MappedEncoding((i:J) => i match {
      case null => None
      case x => Some(d.f(x))
    })


  implicit def udtMapDecodingMapper[V<:Udt](implicit dv: CassandraMapper[UDTValue, V]): CassandraMapper[java.util.Map[java.lang.String,UDTValue], Map[String,V]] =
    CassandraMapper((m: java.util.Map[String,UDTValue]) => if (m == null) Map() else JavaConverters.mapAsScalaMapConverter(m).asScala.map(x => x._1 -> dv.f(new OverridenUdtValue(x._2))).toMap)

  implicit def cassWrapperMapDecodingMapper[J,S](implicit sc: ScalaCassTypeMapper[J,S], d: CassandraMapper[J, S]): CassandraMapper[java.util.Map[java.lang.String,J], Map[String,S]] =
    CassandraMapper((m: java.util.Map[java.lang.String,J]) => if (m == null) Map() else JavaConverters.mapAsScalaMapConverter(m).asScala.map(x => (x._1 -> d.f(x._2))).toMap)


  implicit def udtListDecodingMapper[V<:Udt](implicit dv: CassandraMapper[UDTValue, V]): CassandraMapper[java.util.List[UDTValue], Seq[V]] =
    CassandraMapper((m: java.util.List[UDTValue]) => if (m == null) Seq() else JavaConverters.asScalaBufferConverter(m).asScala.map(x => dv.f(new OverridenUdtValue(x))))

  implicit def cassWrapperListDecodingMapper[J,S](implicit sc: ScalaCassTypeMapper[J,S], d: CassandraMapper[J, S]): CassandraMapper[java.util.List[J], Seq[S]] =
    CassandraMapper((m: java.util.List[J]) => if (m == null) Seq() else JavaConverters.asScalaBufferConverter(m).asScala.map(x => d.f(x)))


  implicit def udtOptionEncodingMapper[V<:Udt](implicit d: CassandraMapper[V,UDTValue]): CassandraMapper[Option[V],UDTValue] =
    CassandraMapper((i:Option[V]) => i match {
      case Some(x) => new OverridenUdtValue(d.f(x))
      case _ => null.asInstanceOf[UDTValue]
    })

  implicit def cassWrapperOptionEncodingMapper[J, S](implicit sc: ScalaCassTypeMapper[J,S], d: CassandraMapper[S,J]): MappedEncoding[Option[S],J] =
    MappedEncoding((i:Option[S]) => i match {
      case Some(x) => d.f(x)
      case _ => null.asInstanceOf[J]
    })


  implicit def udtMapEncodingMapper[V<:Udt](implicit  dv: CassandraMapper[V, UDTValue]): CassandraMapper[Map[String,V],java.util.Map[java.lang.String,UDTValue]] =
    CassandraMapper((m: Map[String,V]) => JavaConverters.mapAsJavaMapConverter(m.map(x => (x._1 -> new OverridenUdtValue(dv.f(x._2)))).toMap[String, UDTValue]).asJava)

  implicit def cassWrapperMapEncodingMapper[J, S](implicit sc: ScalaCassTypeMapper[J,S], d: CassandraMapper[S, J]): CassandraMapper[Map[String,S],java.util.Map[java.lang.String,J]] =
    CassandraMapper((m: Map[String,S]) => JavaConverters.mapAsJavaMapConverter(m.map(x => (x._1 -> d.f(x._2))).toMap[java.lang.String, J]).asJava)

  implicit def udtListEncodingMapper[V<:Udt](implicit  dv: CassandraMapper[V, UDTValue]): CassandraMapper[Seq[V],java.util.List[UDTValue]] =
    CassandraMapper((m: Seq[V]) => JavaConverters.seqAsJavaListConverter[UDTValue](m.map(x => new OverridenUdtValue(dv.f(x)))).asJava)

  implicit def cassWrapperListEncodingMapper[J, S](implicit sc: ScalaCassTypeMapper[J,S], d: CassandraMapper[S, J]): CassandraMapper[Seq[S],java.util.List[J]] =
    CassandraMapper((m: Seq[S]) => JavaConverters.seqAsJavaListConverter(m.map(x => d.f(x))).asJava)


  implicit val byteStringDecodingMapper: MappedEncoding[ByteBuffer, ByteString] =
    MappedEncoding((b: ByteBuffer) =>  ByteString.copyFrom(b))

  implicit val byteStringEncodingMapper: MappedEncoding[ByteString, ByteBuffer] =
    MappedEncoding((s: ByteString) =>  ByteBuffer.wrap(s.toByteArray))


  implicit val timestampDecodingMapper: MappedEncoding[Date, Timestamp] =
    MappedEncoding((i:Date) =>  {
      val instant = i.toInstant
      Timestamp.defaultInstance.withSeconds(instant.getEpochSecond).withNanos(instant.getNano)
    })

  implicit val timestampEncodingMapper: MappedEncoding[Timestamp, Date] =
    MappedEncoding((t:Timestamp) =>  new Date(Instant.ofEpochSecond(t.seconds, t.nanos).toEpochMilli))

  implicit val uuidDecodingMapper: MappedEncoding[UUID, String] =
    MappedEncoding((i:UUID) =>  i.toString)

   implicit val uuidEncodingMapper: MappedEncoding[String, UUID] =
    MappedEncoding((s:String) =>  {
      val childBytes = s.getBytes
      UUID.nameUUIDFromBytes(ByteBuffer.allocate(16 + childBytes.size).
        putLong(rootUUID.getMostSignificantBits).
        putLong(rootUUID.getLeastSignificantBits).
        put(childBytes).array)
    })

  implicit def enumDecodingMapper[T<:CDCEnum]: MappedEncoding[String, T]  = MappedEncoding((i:String) => enums.getOrElse(i, Unspecified).asInstanceOf[T])

  implicit def enumEncodingMapper[T<:CDCEnum]: MappedEncoding[T, String]  = MappedEncoding((e:CDCEnum) => e.toString)

}