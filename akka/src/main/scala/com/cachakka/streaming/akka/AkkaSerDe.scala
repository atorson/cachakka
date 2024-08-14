package com.cachakka.streaming.akka



import java.io.InvalidObjectException
import java.util.concurrent.ConcurrentHashMap

import akka.actor.ExtendedActorSystem
import akka.http.scaladsl.marshalling.{Marshaller, ToEntityMarshaller}
import akka.http.scaladsl.model.MediaTypes
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, Unmarshaller}
import akka.serialization.BaseSerializer
import akka.util.ByteString
import com.fasterxml.jackson.databind.{DeserializationFeature, JsonMappingException, ObjectMapper}
import com.trueaccord.scalapb.json.{Parser, Printer}
import com.trueaccord.scalapb.{GeneratedMessage, GeneratedMessageCompanion, Message}
import org.json4s.JsonAST.JArray
import org.json4s.jackson.JsonMethods

import scala.collection.concurrent.TrieMap

/**
  * SerDe needed by Akka to send sharding messages and instrumental messages over the wire
  * Note: we can only send Proto or JSON messages. Don't expect that an arbitrary class can be used to communicate accross the nodes
  * it will be accepted by Akka - but will be slow because Java SerDe will be used
  * @param system
  */
final class AkkaJsonSerializer(val system: ExtendedActorSystem) extends BaseSerializer {

  protected val mapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  override def includeManifest = true

  override def toBinary(obj: AnyRef) = mapper.writeValueAsBytes(obj)

  override def fromBinary(bytes: Array[Byte], clazz: Option[Class[_]]) = mapper.readValue(bytes, clazz.getOrElse(null)).asInstanceOf[AnyRef]
}


final class AkkaProtoSerializer(val system: ExtendedActorSystem) extends BaseSerializer {

  import collection.JavaConverters._
  import scala.language.existentials
  type Proto[E] = GeneratedMessage with Message[E]
  type AnyP = E forSome{type E<:AnyRef with Proto[E]}
  type AnyCmp = GeneratedMessageCompanion[E] forSome{type E<:AnyRef with Proto[E]}

  val gmc = classOf[GeneratedMessage]
  val ac = classOf[AnyRef]

  private val companionCache = TrieMap[Class[_], AnyCmp]()

  override def includeManifest = true

  override def toBinary(obj: AnyRef): Array[Byte] = {
    obj.getClass match {
     case c: Class[_] if gmc.isAssignableFrom(c) => {
       obj.asInstanceOf[AnyP].toByteArray
     }
     case _ =>   throw new IllegalArgumentException("Need a ScalaPB protobuf message class to be able to de-serialize a protobuf message")
   }
  }

  override def fromBinary(bytes: Array[Byte], clazz: Option[Class[_]]) = {
     clazz match {
      case Some(c: Class[_]) if gmc.isAssignableFrom(c) && ac.isAssignableFrom(c) => {
        val cmp = companionCache.getOrElseUpdate(c, findCompanion[AnyCmp](c).get)
        cmp.parseFrom(bytes)
      }
      case _ =>   throw new IllegalArgumentException("Need a ScalaPB protobuf message class to be able to serialize bytes using protobuf")
    }
  }
}

object AkkaProtoHttpSupport {

  implicit class PreserveProtoFieldNames(val flag: Boolean)

  protected val preservingProtoFieldNames = true

  protected val parser_preserve_true = new Parser(true)
  protected val printer_preserve_true = new Printer(preservingProtoFieldNames = true)
  protected val parser_preserve_false = new Parser(false)
  protected val printer_preserve_false = new Printer(preservingProtoFieldNames = false)



  protected def getParser(preserveProtoFieldNames: PreserveProtoFieldNames): Parser = {
    preserveProtoFieldNames.flag match {
      case false => parser_preserve_false
      case _ => parser_preserve_true
    }
  }

  protected def getPrinter(preserveProtoFieldNames: PreserveProtoFieldNames): Printer = {
    preserveProtoFieldNames.flag match {
      case false => printer_preserve_false
      case _ => printer_preserve_true
    }
  }

  private val jsonStringUnmarshaller =
  Unmarshaller.byteStringUnmarshaller
  .forContentTypes(MediaTypes.`application/json`)
  .mapWithCharset {
  case (ByteString.empty, _) => throw Unmarshaller.NoContentException
  case (data, charset)       => data.decodeString(charset.nioCharset.name)
}

  private val jsonStringMarshaller =
  Marshaller.stringMarshaller(MediaTypes.`application/json`)


  /**
    * HTTP entity => `A`
    *
    * @tparam E type to decode
    * @return unmarshaller for `A`
    */
  def jsonProtoUnmarshaller[E<:GeneratedMessage with Message[E]](preserveProtoFieldNames: PreserveProtoFieldNames = preservingProtoFieldNames)(implicit cmp: GeneratedMessageCompanion[E]): FromEntityUnmarshaller[E] =

    jsonStringUnmarshaller.map(fromJsonStringTransformer[E](preserveProtoFieldNames)).recover(
      _ =>
        _ => {
          case x: JsonMappingException =>
            throw x.getCause
        }
    )


  /**
    * `A` => HTTP entity
    *
    * @tparam E type to encode, must be upper bounded by `AnyRef`
    * @return marshaller for any `A` value
    */
  def jsonProtoMarshaller[E<:GeneratedMessage with Message[E]](preserveProtoFieldNames: PreserveProtoFieldNames = preservingProtoFieldNames) : ToEntityMarshaller[E] = jsonStringMarshaller.compose(toJsonStringTransformer[E](preserveProtoFieldNames))


  def fromJsonStringTransformer[E<:GeneratedMessage with Message[E]](preserveProtoFieldNames: PreserveProtoFieldNames = preservingProtoFieldNames)(implicit cmp: GeneratedMessageCompanion[E]) =
  {data: String => getParser(preserveProtoFieldNames).fromJsonString[E](data)}


  def toJsonStringTransformer[E<:GeneratedMessage with Message[E]](preserveProtoFieldNames: PreserveProtoFieldNames = preservingProtoFieldNames) =
  {data: E => getPrinter(preserveProtoFieldNames).print[E](data)}



  /**
    * `TraversableOnce[E]` => HTTP entity
    *
    * @tparam E type to encode, must be upper bounded by `AnyRef`
    * @return marshaller for any `E` value
    */
  def jsonProtoArrayMarshaller[E<:GeneratedMessage with Message[E]](preserveProtoFieldNames: PreserveProtoFieldNames = preservingProtoFieldNames) : ToEntityMarshaller[List[E]] = {
    jsonStringMarshaller.compose[List[E]](collectionToJsonString[E](preserveProtoFieldNames))
  }

  /**
    * HTTP entity => TraversableOnce[E] =>
    *
    * @tparam E type to encode, must be upper bounded by `AnyRef`
    * @return unmarshaller for any `E` value
    */
  def jsonProtoArrayUnmarshaller[E <: GeneratedMessage with Message[E]](preserveProtoFieldNames: PreserveProtoFieldNames = preservingProtoFieldNames)(implicit cmp: GeneratedMessageCompanion[E]): FromEntityUnmarshaller[List[E]] =
    jsonStringUnmarshaller.map(data => {
      JsonMethods.parse(data) match {
        case JArray(arr) => arr.map(getParser(preserveProtoFieldNames).fromJson[E](_))
        case _ => throw JsonMappingException.fromUnexpectedIOE(new InvalidObjectException(s"JSON parsing of $data did not produce JArray, as expected"))
      }
    }).recover(
      _ =>
        _ => {
          case x: JsonMappingException =>
            throw x.getCause
        }
    )

  private final def collectionToJsonString[E <: GeneratedMessage with Message[E]](preserveProtoFieldNames: PreserveProtoFieldNames = preservingProtoFieldNames)(c: List[E]): String = {
    import org.json4s.jackson.JsonMethods._
    compact(JArray(c.map(getPrinter(preserveProtoFieldNames).toJson[E])))
  }

  /*import collection.JavaConverters._
  import Ordering._

  private val edParentID = ID(ID.UtilityCategory, "ED")

  protected def id(fields: Seq[String]) = ID(edParentID, fields.toString)

  protected val definitionCache = new ConcurrentMapSimpleRegistry[ED](Reg.cbBuilder[ED].build())

  protected lazy val entityDefinitions: Set[(Set[String], ED)] =
    EntityDefinition.all().map(x=>(iterableAsScalaIterableConverter(x.companion.descriptor.getFields).asScala.map(_.getJsonName).toSet, x))

  protected def getEntityDefinition(fields: Seq[String], onFailure: => Throwable): ED = {
    definitionCache  ? (Reg.GetOrAdd[ED], id(fields),
      Some({ () => {
        val nameset = fields.toSet
        entityDefinitions.find(x => nameset.diff(x._1).isEmpty) match{
          case Some(v) => Some(v._2)
          case _ =>  throw onFailure
        }
      }})) match {
      case Success(Some(fd)) => fd
      case _ => throw onFailure
    }
  }

  protected def fromClassTag[E <: GeneratedMessage with Message[E]](value: JValue)(implicit cmp: GeneratedMessageCompanion[E]): E = {
    JsonFormat.fromJson[E](value)
  }


  val genericJsonProtoUnmarshaller: FromEntityUnmarshaller[BE] =
    jsonStringUnmarshaller.map { data => {
      val v = JsonMethods.parse(data)
      v match {
        case JObject(fields) => {
          val definition = getEntityDefinition(fields.map(_._1).sorted, JsonMappingException.fromUnexpectedIOE(
            new InvalidObjectException(s"JSON object field set $fields did not match any registered base entity")))
          fromClassTag(v)(definition.companion).asInstanceOf[BE]
        }
        case _ =>  throw JsonMappingException.fromUnexpectedIOE(new InvalidObjectException(s"JSON parsing of $data did not produce JObject, as expected"))
      }

    }}.recover(
      _ =>
        _ => {
          case x: JsonMappingException  =>
            throw x.getCause
        }
    )

  */
}








