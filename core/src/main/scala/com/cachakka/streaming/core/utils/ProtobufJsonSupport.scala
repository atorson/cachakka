package com.cachakka.streaming.core.utils




import java.time.Instant
import java.util.concurrent.atomic.AtomicReference
import java.util.function
import java.util.function.{BiFunction, BinaryOperator}

import com.fasterxml.jackson.databind.JsonNode
import com.google.common.base.{CaseFormat, Preconditions, Strings}
import com.google.protobuf.timestamp.Timestamp
import com.google.protobuf.wrappers._
import com.trueaccord.scalapb.json.{JsonFormat, Parser, Printer}
import com.trueaccord.scalapb.{GeneratedMessage, GeneratedMessageCompanion, Message}
import com.typesafe.scalalogging.LazyLogging
import com.cachakka.streaming.core.CoreProto.ingressJsonName
import com.cachakka.streaming.core.api.{StreamingEntity, StreamingEntityCompanion}
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, ISODateTimeFormat}
import org.json4s.JField
import org.json4s.JsonAST._
import org.json4s.jackson.JsonMethods

import scala.util.{Failure, Success, Try}
import scalapb.descriptors._


trait StreamingProtoToJsonConfig {

  def preserveProtoNamesInJson(): Boolean

}

trait SnakeCaseProtoToJsonConfig extends StreamingProtoToJsonConfig{

  override def preserveProtoNamesInJson() = true

}


trait CamelCaseProtoToJsonConfig extends StreamingProtoToJsonConfig{

  override def preserveProtoNamesInJson() = false

}


object ProtobufJsonSupport extends LazyLogging {

  import scala.language.existentials

  implicit class PreserveProtoFieldNames(val flag: Boolean){}

  private val ISO_GENERIC = ISODateTimeFormat.dateTimeParser().withZone(Utils.TIME_ZONE) // must include 'T'
  private val DEFAULT = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS Z").withZone(Utils.TIME_ZONE)
  private val MY_SQL = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZone(Utils.TIME_ZONE)

  protected val parser_preserve_true = new Parser(true)
  protected val printer_preserve_true = new Printer(preservingProtoFieldNames = true)
  protected val parser_preserve_false = new Parser(false)
  protected val printer_preserve_false = new Printer(preservingProtoFieldNames = false)


  type JsonPathSegment = (String, String) // simple type name + short field name
  type JsonPath = Seq[JsonPathSegment] // full JSON path in nested JSON object models
  type JsonSignature = Set[JsonPath] // flattened set of all possible JSON paths for a JSON object tree
  type JsonField = (JsonPath, JValue) // differs from JField and uses a full JSON path instead of a short field name
  type ProtoPathSegment = FieldDescriptor // field descriptor has a complete info about field (number, name, type)
  type ProtoPath = Seq[ProtoPathSegment] // full Proto path in nested Proto object models
  type J2P = (ProtoPath, JValue => JValue) // info we need for conversion of JsonField: full Proto path to a matching Proto field and adaptor of JValue
  type JM = (Map[JsonPath, J2P], Map[ProtoPath, Seq[JsonPath]]) //map of particular JSON fields to Proto fields + inverse map of particular Proto fields to JSON fields (ordered by descending degree of matching)
  type JV = (JsonSignature, JM) // cached entity format for any Streaming Entity: full signature of JSON model + direct mapping + inverse mapping

  // cached mapping of json and proto fields
  // key = (proto classname, cached entity format
  // need Atomic reference to handle concurrency (mutiple entities at the same time or same entity concurrent calls

  protected val json2protoFieldMap: AtomicReference[Map[String, JV]] = new AtomicReference(Map[String, JV]())

  // need this existential for reads() calls (they are tigtly bound, wont accept GeneratedMessageCompanion[_])
  type C = GeneratedMessageCompanion[E] forSome {type E <: GeneratedMessage with Message[E]}

  /**
    * Marshalls Proto streaming entity to JSON string
    *
    * @return JSON string
    */
  def toJsonString[E <: StreamingEntity[_, E]](data: E, compact: Boolean = true) = {
    val jsonObject = getPrinter(data.getCompanion).toJson(data)
    compact match {
      case true => JsonMethods.compact(jsonObject)
      case _ => JsonMethods.pretty(jsonObject)
    }
  }

  /**
    * Marshalls Proto streaming entity to JSON object
    *
    * @return JSON object
    */
  def toJsonNode[E <: StreamingEntity[_, E]](data: E) = JsonMethods.asJsonNode(getPrinter(data.getCompanion).toJson(data))

  /**
    * Unmarshalls Proto streaming entity from a JSON string
    *
    * @param data JSON string
    * @param cmp  streaming entity companion (contains Proto descriptor/schema)
    * @tparam E streaming entity type
    * @return streaming entity
    */
  def fromJsonString[E <: StreamingEntity[_, E]](data: String, exactMatchFirst: Boolean = true, recoverOnFailure: Boolean = true)(implicit cmp: StreamingEntityCompanion[_, E]): E = fromJValue(JsonMethods.parse(data), exactMatchFirst, recoverOnFailure);

  /**
    * Unmarshalls Proto streaming entity from a JSON object
    *
    * @param data JSON object
    * @param cmp  streaming entity companion (contains Proto descriptor/schema)
    * @tparam E streaming entity type
    * @return streaming entity
    */
  def fromJsonNode[E <: StreamingEntity[_, E]](data: JsonNode, exactMatchFirst: Boolean = true, recoverOnFailure: Boolean = true)(implicit cmp: StreamingEntityCompanion[_, E]): E = {
    fromJValue(JsonMethods.fromJsonNode(data), exactMatchFirst, recoverOnFailure)
  }

  /**
    * Tries to read exactly
    * If it fails, tries to read inexactly (using smart matching, with or without cache)
    */
  def fromJValue[E <: StreamingEntity[_, E]](data: JValue, exactMatchFirst: Boolean, recoverOnFailure: Boolean)(implicit cmp: StreamingEntityCompanion[_, E]) = {
    Try(if (exactMatchFirst) {
      getParser(cmp).fromJson(data)
    } else {
      matchJValue(data)(cmp)
    }).recover[E]{ case x => {
      if (recoverOnFailure) {
        logger.debug(s"Recovering on JSON $data read failure: $x")
        if (exactMatchFirst) {
          matchJValue(data)(cmp)
        } else {
          JsonFormat.fromJson(data)
        }
      } else {
        throw x
      }
    }
    }.get
  }

  protected def getParser[E <: StreamingEntity[_, E]](cmp: StreamingEntityCompanion[_, E]): Parser = getParser(cmp.preserveProtoNamesInJson())

  protected def getParser(preserveProtoFieldNames: Boolean): Parser = {
    preserveProtoFieldNames match {
      case false => parser_preserve_false
      case _ => parser_preserve_true
    }
  }

  protected def getPrinter[E <: StreamingEntity[_, E]](cmp: StreamingEntityCompanion[_, E]): Printer = {
    getPrinter(cmp.preserveProtoNamesInJson())
  }

  protected def getPrinter(preserveProtoFieldNames: Boolean): Printer = {
    preserveProtoFieldNames match {
      case false => printer_preserve_false
      case _ => printer_preserve_true
    }
  }

  protected def getJsonFieldNameExtractor[E <: StreamingEntity[_, E]](cmp: StreamingEntityCompanion[_, E]): FieldDescriptor => Set[String] = {
    getJsonFieldNameExtractor(cmp.preserveProtoNamesInJson())
  }

  protected def getJsonFieldNameExtractor(preserveProtoFieldNames: Boolean): FieldDescriptor => Set[String] = { fd: FieldDescriptor => {
    val snakeToCamelConverter = CaseFormat.LOWER_UNDERSCORE.converterTo(CaseFormat.LOWER_CAMEL)
    val ingressJsonNameValue = fd.getOptions.extension(ingressJsonName)
    val jsonName = preserveProtoFieldNames match {
      case false => fd.asProto.jsonName.getOrElse(snakeToCamelConverter.convert(fd.name))
      case _ => fd.name
    }
    if (Strings.isNullOrEmpty(ingressJsonNameValue)) Set(jsonName) else Set(jsonName, ingressJsonNameValue)
  }}


  private def matchJValue[E <: StreamingEntity[_, E]](data: JValue)(cmp: StreamingEntityCompanion[_, E]) = {
    val preserveProtoFieldNames: PreserveProtoFieldNames = cmp.preserveProtoNamesInJson()
    data match {
      case v: JObject => {
        val defaultInstance = cmp.defaultInstance
        var result = defaultInstance.asInstanceOf[E]
        val clazzKey = defaultInstance.getClass.getName
        val oldCachedEntryOption = json2protoFieldMap.get().get(clazzKey)
        val jsonFields = extractAllJsonFields(Seq[JsonPathSegment](), v, oldCachedEntryOption)
        val jsonSignature = jsonFields.map(_._1).toSet
        lazy val emptyCachedEntry: JV = (Set[JsonPath](), (Map[JsonPath, J2P](), Map[ProtoPath, Seq[JsonPath]]()))
        var oldCachedEntry = oldCachedEntryOption.getOrElse(emptyCachedEntry)
        val needToMatch = !jsonSignature.subsetOf(oldCachedEntry._1) ||  Try {
            // we can use cache safely without matching: all fields in JSON signature have been observed
            result = jsonFields.foldLeft(defaultInstance)((curr, jf) => enrichFromCachedEntry(jf, jsonSignature, oldCachedEntry, curr)(preserveProtoFieldNames).asInstanceOf[E])
          }
         .map(_ => false).recover{case x: Throwable => {
            logger.error(s"Caught exception while parsing JSON data $data to instance of the Proto class $clazzKey using cached mappings: $x")
            // invalidate the cached entry on exceptions
            json2protoFieldMap.getAndAccumulate(Map[String, JV]((clazzKey -> emptyCachedEntry)),
              ProtobufJsonImplicits.toJavaBiOperator((prev, x) => x))
            oldCachedEntry = emptyCachedEntry
            true
          }}.get
        if (needToMatch) {
          // need to match and update cached mappings
          val r = matchObjectAndEnrichEntity((Seq[JsonPathSegment](), v))((Seq[ProtoPathSegment](), cmp.defaultInstance), Some(emptyCachedEntry._2))(preserveProtoFieldNames)
          val cachedEntry: JV = (jsonSignature, r._2.getOrElse(oldCachedEntry._2))
          // merge changes into cache (consider concurrency)
          json2protoFieldMap.getAndAccumulate(Map[String, JV]((clazzKey -> cachedEntry)),
            ProtobufJsonImplicits.toJavaBiOperator((prev, x) => {
              val clazzName = x.head._1
              val newEntry = x.head._2
              prev.get(clazzKey) match {
                case Some(oldEntry) => {
                  val mergedEntry: JV = ((oldEntry._1 ++ newEntry._1) -> mergeProtoMappings(oldEntry._2, newEntry._2))
                  prev + (clazzName -> mergedEntry)
                }
                case _ => prev + (clazzName -> newEntry)
              }
            }))
            val ne =  json2protoFieldMap.get().get(clazzKey).getOrElse(emptyCachedEntry)
            val needToUseMatchedResult = !jsonSignature.subsetOf(ne._1) || Try{
               // re-run the result based on cache
              result = jsonFields.foldLeft(defaultInstance)((curr, jf) => enrichFromCachedEntry(jf, jsonSignature, ne, curr)(preserveProtoFieldNames).asInstanceOf[E])
            }
            .map(_ => false).recover{case x: Throwable => {
              logger.error(s"Caught exception while parsing JSON data $data to instance of the Proto class $clazzKey using cached mappings: $x")
              // invalidate the cached entry on exceptions
              json2protoFieldMap.getAndAccumulate(Map[String, JV]((clazzKey -> emptyCachedEntry)),
                ProtobufJsonImplicits.toJavaBiOperator((prev, x) => x))
               true
            }}.get
            if (needToUseMatchedResult) result = r._1.fold(identity, identity).asInstanceOf[E]
        }
        result
      }
      case _ => throw new IllegalArgumentException(s"Failed to parse $data into an instance of ${cmp.defaultInstance.getClass}")
    }
  }

  private def mergeProtoMappings(oldM: JM, newM: JM): JM = {
    val directMapMergedList = newM._1.toList ++ oldM._1.toList
    val directMapMerged = Map[JsonPath, J2P]() ++ directMapMergedList.groupBy(_._1).map(c => c._1 ->
       // get the most perfect match in case of conflicts (in case of ties, trust new)
       c._2.foldLeft[J2P](c._2.head._2)((x,s) => if (isPerfectMatch(c._1, s._2._1.last)) s._2 else x)
    )
    val indirectMapMergedList = newM._2.toList ++ oldM._2.toList
    val indirectMapMerged = Map[ProtoPath, Seq[JsonPath]]() ++ indirectMapMergedList.groupBy(_._1).map(c => c._1 ->
      c._2.foldLeft(Seq[JsonPath]())((x, s) => s._2.foldLeft[Seq[JsonPath]](x)((y, z) =>
        // must be unique in sequence and consistent with direct map (new comes first)
        if (!y.contains(z) && directMapMerged.get(z).filter(_._1 == c._1).isDefined) (y :+ z) else y))
        .sortWith((p1,p2) => {
          // sort according to best match
          val b1 = isPerfectMatch(p1, c._1.last)
          val b2 = isPerfectMatch(p2, c._1.last)
          if (b1 == b2) false else if (b1) true else false
        })
    )
    (directMapMerged, indirectMapMerged)
  }

  private def enrichFromCachedEntry(data: JsonField, signature: JsonSignature, cachedEntry: JV, instance: GeneratedMessage)(preserveProtoFieldNames: PreserveProtoFieldNames): GeneratedMessage = {
    Try{cachedEntry._2._1.get(data._1) match {
      case Some(e) => {
        // JSON field is mapped to a Proto field
        var shouldMutate = true
        val s: Seq[JsonPath] = cachedEntry._2._2.get(e._1).get
        // got the full set of JSON path mappings for this Proto
        if (s.head != data._1){
          // this JSON field is not the best match
          val inter = s.toSet.intersect(signature)
          if (inter.size > 1) {
            // there are other JSON fields present in the signature - need to see if they are better matches
            val s1: JsonPath = s.find(inter.contains(_)).get
            if (s1 != data._1) {
              // another field present in the JSON signature is the best match
              shouldMutate = false
            }
          }
        }
        if (shouldMutate)  {
          mutateProtoEntity(e._1, e._2(data._2), instance)(preserveProtoFieldNames)
        } else {instance}
      }
      case _ => instance
    }}.recover[GeneratedMessage]{
      case x:Throwable => {
        logger.error(s"Caught exception while updating Proto instance $instance with JSON field data $data: $x")
        throw x
      }
    }.get
  }

  private def getPreserveFieldNamesFlag(cmp: GeneratedMessageCompanion[_], default: PreserveProtoFieldNames): PreserveProtoFieldNames ={
    cmp match {
      case c: StreamingProtoToJsonConfig => c.preserveProtoNamesInJson()
      case _ => default
    }
  }

  /**
    * Recursive method to mutate a Proto entity with a given JSON value
    *
    * @param protoPath full Proto path
    * @param value     JSON value
    * @param instance  old instance
    * @return new instance
    */
  private def mutateProtoEntity(protoPath: ProtoPath, value: JValue, instance: GeneratedMessage)(preserveProtoFieldNames: PreserveProtoFieldNames): GeneratedMessage = {

    def splitPath(path: ProtoPath): Either[(ProtoPathSegment, ProtoPath), ProtoPathSegment] = {
      if (path.isEmpty) throw new IllegalArgumentException("Can not split an empty Proto path")
      else if (path.size == 1) Right(path.head) else Left(path.head, path.tail)
    }

    val cmp = instance.companion.asInstanceOf[C]

    splitPath(protoPath) match {
      //need to use recursion to reach the leaf of the ProtoPath
      case Left(Tuple2(fd, childPath)) => {
        val childCmp = safeGetFieldCompanion(cmp, fd.number).getOrElse(null).asInstanceOf[C]

        if (childCmp == null) throw new IllegalStateException(s"Malformed Proto path $protoPath")

        val childValue = instance.getField(fd) match {
          case v: PMessage => v.as(childCmp.messageReads)
          case _ => childCmp.defaultInstance
        }

        val newChildValue = mutateProtoEntity(childPath, value, childValue)(getPreserveFieldNamesFlag(cmp, preserveProtoFieldNames)).toPMessage
        innerMutateProtoEntity(instance, fd, newChildValue)
      }
      // terminal case
      case (Right(fd)) => {
        val newFieldValueOption = convertJsonFieldValueToProto(value, (fd -> cmp))(getPreserveFieldNamesFlag(cmp, preserveProtoFieldNames))
        newFieldValueOption match {
          case Some(newValue) => innerMutateProtoEntity(instance, fd, newValue)
          case _ => instance
        }
      }
    }
  }

  private def innerMutateProtoEntity(current: GeneratedMessage, fd: FieldDescriptor, newValue: PValue): GeneratedMessage =
    current.getField(fd) match {
       case z: PRepeated =>
         current.companion.messageReads.read(PMessage(current.toPMessage.value + (fd -> PRepeated(z.value
           :+ newValue)))).asInstanceOf[GeneratedMessage]

       case _ =>  current.companion.messageReads.read(PMessage(current.toPMessage.value + (fd -> newValue))).asInstanceOf[GeneratedMessage]
   }


  /**
    * Coverts a JSON field to a Proto field, with smart type conversion
    *
    * @param jsonValue      JSON value
    * @param protoFieldMeta proto field meta data
    * @return
    */
  private def convertJsonFieldValueToProto(jsonValue: JValue, protoFieldMeta: (FieldDescriptor, GeneratedMessageCompanion[_]))(preserveProtoFieldNames: PreserveProtoFieldNames): Option[PValue] = {

    // inner function that relaxes type bounds on the underlying parse API
    def innerFromJson(value: JValue, onError: => PValue)(cmp: GeneratedMessageCompanion[_]) = {

      Try(cmp match {
          case c: StreamingEntityCompanion[_,_] => ProtobufJsonSupport.fromJValue(value, false, true)(c)
          case _ =>  getParser(getPreserveFieldNamesFlag(cmp, preserveProtoFieldNames).flag).fromJson(value)(cmp.asInstanceOf[C]).asInstanceOf[GeneratedMessage]
         }).map(_.toPMessage).getOrElse(onError)
    }

    def parseDateString(date: String): Try[DateTime] = Try{ISO_GENERIC.parseDateTime(date)}
      .recoverWith{case x => Try{DEFAULT.parseDateTime(date)}
        .recoverWith{case x => Try{MY_SQL.parseDateTime(date)}}}

    def parseDateLong(millis: Long): Try[DateTime] = Success(new DateTime(millis, Utils.TIME_ZONE))

    def parseDateInt(millis: BigInt): Try[DateTime] = parseDateLong(millis.toLong)

    def convert(dateTime: DateTime): Timestamp = {
      val i = Instant.ofEpochMilli(dateTime.getMillis)
      Timestamp.defaultInstance.withSeconds(i.getEpochSecond).withNanos(i.getNano)
    }

    def onFailure: PValue = {
      throw new NullPointerException
    }

    val protoField = protoFieldMeta._1

    lazy val childCmp = safeGetFieldCompanion(protoFieldMeta._2, protoField.number)

    lazy val onError = {
      logger.debug(s"JSON field with value $jsonValue matched the name but mis-matched the type of the field ${protoField}")
      PEmpty
    }

    val result = Tuple3(jsonValue, protoField.isRepeated, protoField.scalaType) match {
      // complex object
      case (value: JObject, false, ScalaType.Message(_)) => innerFromJson(value, onError)(childCmp.getOrElse(null))
      // object field mismatch
      case (value: JObject, _, _) => onError
      // collection of objects
      case (value: JArray, true, ScalaType.Message(_)) => Try(PRepeated(value.arr.map(innerFromJson(_, onFailure)(childCmp.getOrElse(null))).toVector)).getOrElse(onError)
      // collection of primitives
      case (value: JArray, true, t) => Try(PRepeated(value.arr.map(JsonFormat.parsePrimitiveByScalaType(t, _, onFailure)).toVector)).getOrElse(onError)
      // array field mismatch
      case (value: JArray, _, _) => onError
      // collection of objects
      case (value: JSet, true, ScalaType.Message(_)) => Try(PRepeated(value.set.map(innerFromJson(_, onFailure)(childCmp.getOrElse(null))).toVector)).getOrElse(onError)
      // collection of primitives
      case (value: JSet, true, t) => Try(PRepeated(value.set.map(JsonFormat.parsePrimitiveByScalaType(t, _, onFailure)).toVector)).getOrElse(onError)
      // array field mismatch
      case (value: JSet, _, _) => onError
      // Primitive wrappers
      case (value: JValue, false, ScalaType.Message(d)) if (d.fullName.startsWith("google.protobuf") && d.fullName.endsWith("Value")) => value match {
        case v: JString => StringValue.defaultInstance.withValue(v.s).toPMessage
        case v: JLong => Int64Value.defaultInstance.withValue(v.num).toPMessage
        case v: JInt => Int32Value.defaultInstance.withValue(v.num.intValue()).toPMessage
        case v: JBool => BoolValue.defaultInstance.withValue(v.value).toPMessage
        case v: JDouble => DoubleValue.defaultInstance.withValue(v.num).toPMessage
        case v: JDecimal => Int32Value.defaultInstance.withValue(v.num.intValue()).toPMessage
        case _ => onError
      }

      // string field : could be a timestamp
      case (value: JString, false, ScalaType.Message(_)) => childCmp match {
        case Some(Timestamp) => parseDateString(value.s).map(convert(_).toPMessage).getOrElse(onError)
        case _ => onError
      }
      // stringified primitive
      case (value: JString, false, t) => {
        Try(Preconditions.checkNotNull(t match {
          case ScalaType.Int => PInt(java.lang.Integer.parseInt(value.s))
          case ScalaType.Long => PLong(java.lang.Long.parseLong(value.s))
          case ScalaType.Double => PDouble(java.lang.Double.parseDouble(value.s))
          case ScalaType.Float => PFloat(java.lang.Float.parseFloat(value.s))
          case ScalaType.Boolean => PBoolean(Try(java.lang.Boolean.parseBoolean(value.s)).getOrElse(!value.s.isEmpty && !value.s.equalsIgnoreCase("0")))
          case ScalaType.Enum(d) =>{
            val convertedName = toANLC(value.s)
            PEnum(d.values.find(v => {
              val t = toANLC(v.name)
              if (t.contains(convertedName)){
                val l = v.name.toLowerCase
                val l1 = value.s.toLowerCase
                (l == l1 || l.contains(s"_${l1}") || l.contains(s"${l1}_"))
              } else false
            }).get)
          }
          case _ => JsonFormat.parsePrimitiveByScalaType(t, value, onError)
        })).getOrElse(onError)
      }
      // Long field: could be a timestamp
      case (value: JLong, false, ScalaType.Message(_)) => childCmp match {
        case Some(Timestamp) => parseDateLong(value.num).map(convert(_).toPMessage).getOrElse(onError)
        case _ => onError
      }
      // Int field: could be a timestamp
      case (value: JInt, false, ScalaType.Message(_)) => childCmp match {
        case Some(Timestamp) => parseDateInt(value.num).map(convert(_).toPMessage).getOrElse(onError)
        case _ => onError
      }
      // primitive: mismatch
      case (value: JValue, false, ScalaType.Message(_)) => onError
      // strongly-typed/true primitive
      case (value: JValue, false, t) => JsonFormat.parsePrimitiveByScalaType(t, value, onError)
      case _ => onError
    }
    if (result == PEmpty) None else Some(result)
  }

  private def nextObject(v: JValue): Option[JObject] = {
    v match {
      case x: JObject => Some(x)
      case JArray(arr) if (!arr.isEmpty) => nextObject(arr.head)
      case JSet(set) if (!set.isEmpty) => nextObject(set.head)
      case _ => None
    }
  }

  private def isSet(v: JValue): Boolean = {
    v match {
      case JObject(obj) => obj != null && !obj.isEmpty
      case JArray(arr) => !arr.isEmpty
      case JSet(set) => !set.isEmpty
      case JString(s) => !Strings.isNullOrEmpty(s)
      case JInt(i) => i != null
      case JDecimal(dec) => dec != null
      case JNull => false
      case JNothing => false
      case _ => true
    }
  }

  private def extractAllJsonFields(prefix: JsonPath, data: JObject, cachedEntry: Option[JV]): Seq[JsonField] = {
    data.obj.foldLeft(Seq[JsonField]())((curr, jf) => {
      if (isSet(jf._2)) {
        val suffix = (jf._2.getClass.getSimpleName -> jf._1)
        val path = prefix :+ suffix
        val next = curr :+ (path -> jf._2)
        nextObject(jf._2) match {
          // a small performance optimization: if an object mapping has been matched - no need to expand the objects fields (so will use a reduced JSON signature)
          case Some(x) => if (cachedEntry.map(_._2._1.keySet.contains(path)).getOrElse(false)) next else (next ++ extractAllJsonFields(path, x, cachedEntry))
          case _ => next
        }
      } else {curr}
    })
  }

  private def safeGetFieldCompanion(cmp: GeneratedMessageCompanion[_], number: Int): Option[GeneratedMessageCompanion[_]] = {
    Try{Option[GeneratedMessageCompanion[_]](cmp.messageCompanionForFieldNumber(number))}.getOrElse(None)
  }


  /**
    * Recursively iterates over JSON fields, searches for a matching Proto field and updates the Proto, if matched
    * Does not use caching - but populates it as a result of matching
    */
  private def matchObjectAndEnrichEntity(data: (JsonPath, JObject))(target: (ProtoPath, GeneratedMessage), cachedEntry: Option[JM])(preserveProtoFieldNames: PreserveProtoFieldNames): (Either[GeneratedMessage, GeneratedMessage], Option[JM]) = {
    val clazz = target._2.getClass
    val protoPath = target._1
    val prefix = data._1
    val jo = data._2

    // temp caches
    var runningResult: (Either[GeneratedMessage, GeneratedMessage], Option[JM]) = (Left(target._2), cachedEntry)
    var path: JsonPath = prefix
    var protoOpt: Option[ProtoPath] = None
    var transformer: JValue => JValue = identity(_)
    var matchedProtoFields = cachedEntry.map(_._2.keySet.map(_.last)).getOrElse(Set[FieldDescriptor]())

    // inner functions, executing a fallback call with side-effects depending on the outcome of Either
    def verifyFieldResult(newValue: Either[(Option[Either[ProtoPath,ProtoPath]],GeneratedMessage), (ProtoPath,GeneratedMessage)])(onLeft: => Unit): Unit =
      newValue match {
        case Right(v) => {
          matchedProtoFields = matchedProtoFields + v._1.last
          runningResult = (Right(v._2), runningResult._2.map(v1 => mergeProtoMappings(v1, {
            (Map[JsonPath, J2P](path -> (v._1, transformer)),Map[ProtoPath, Seq[JsonPath]](v._1 -> Seq(path)))
          })))
        }
        case Left(t) => {
          protoOpt = t._1.map(_.fold(identity(_),identity(_)))
          if (t._1.isDefined && t._1.get.isRight){
            // imperfect match: need to update running result's mappings and not execute onLeft callback
            val v = protoOpt.get
            runningResult = (Left(target._2), runningResult._2.map(v1 => mergeProtoMappings(v1, {
              (Map[JsonPath, J2P](path -> (v, transformer)), Map[ProtoPath, Seq[JsonPath]](v -> (v1._2.get(v).getOrElse(Seq[JsonPath]()) :+ path)))
            })))
          } else onLeft
        }
      }

    def verifyObjectResult(newValue: (Either[GeneratedMessage, GeneratedMessage], Option[JM]))(onLeft: => Unit): Unit =
      newValue match {
        case (Right(v), o) => {
          matchedProtoFields = matchedProtoFields ++ o.map(_._2.keySet.map(_.last)).getOrElse(Set[FieldDescriptor]())
          runningResult = (Right(v), runningResult._2.flatMap(v1 => o.map(v2 => mergeProtoMappings(v1,v2))))
        }
        case _ => onLeft
      }

    def unwrap() = runningResult._1.fold[GeneratedMessage](identity(_),identity(_))

    // first attempt: match each field to a Proto field
    jo.obj.filter(f => isSet(f._2)).foreach(jf => {
        path = prefix :+ (jf._2.getClass.getSimpleName -> jf._1)
        transformer = identity(_)
        protoOpt = None
        verifyFieldResult(matchFieldAndEnrichEntity(path -> jf._2)(unwrap(), protoPath, matchedProtoFields)(preserveProtoFieldNames)) {
        logger.debug(s"Field $jf did not match schema of $clazz")
        jf._2 match {
          // field itself is a complex object
          case v: JObject => {
            def onObjLeft(): Unit =  {
            // try to match as a singleton collection
            transformer = x => JArray(List(x))
            verifyFieldResult(matchFieldAndEnrichEntity(path -> transformer(v))(unwrap(), protoPath, matchedProtoFields)(preserveProtoFieldNames)) {
              logger.debug(s"Could not match singleton collection ${v.toString} with field name ${jf._1} to schema of $clazz")
              // try to match the whole object as a string with the same field name
              transformer = x => JString(JsonMethods.compact(x))
              verifyFieldResult(matchFieldAndEnrichEntity(path -> transformer(v))(unwrap(), protoPath, matchedProtoFields)(preserveProtoFieldNames)) {
                logger.debug(s"Could not match stringified ${v.toString} with field name ${jf._1} to schema of $clazz")
              }
            }}
            if (protoOpt.isEmpty)
              // no proto field match: try to match this object field-by-field, recursively
              verifyObjectResult(matchObjectAndEnrichEntity((path,v))((protoPath, unwrap()), runningResult._2)(preserveProtoFieldNames)) {
              logger.debug(s"No fields of $v matched schema of $clazz")
              onObjLeft()
            }  else onObjLeft()
          }
          // field is an array
          case v: JArray if !v.arr.isEmpty => {
            // fuzzy match on collections that do not match to Proto collections
            // try to match the whole collection as a string with the same field name: only works if the matching Proto field is a string
            transformer = x => JString(JsonMethods.compact(x))
            verifyFieldResult(matchFieldAndEnrichEntity(path -> transformer(v))(unwrap(), protoPath, matchedProtoFields)(preserveProtoFieldNames)) {
              logger.debug(s"Could not match stringified collection ${v.arr.toString} with field name ${jf._1} to schema of $clazz")
              val v1 = v.arr.head
              val allSame = v.arr.foldLeft[Boolean](true)((b, z) => b && (v1 == z))
              transformer = x => x.asInstanceOf[JArray].arr.head
              (allSame, nextObject(v1)) match {
                // collections of identical objects:
                case (true, Some(z)) => {
                  // first try to match the head as a whole object with the same field name
                  verifyFieldResult(matchFieldAndEnrichEntity(path -> transformer(v))(unwrap(), protoPath, matchedProtoFields)(preserveProtoFieldNames)) {
                    logger.debug(s"Collection head field ${jf._1 -> z} did not match schema of $clazz")

                    def onArrayLeft(): Unit = {
                      // try to match the head as a string with the same field name
                      transformer = x => JString(JsonMethods.compact(x.asInstanceOf[JArray].arr.head))
                      verifyFieldResult(matchFieldAndEnrichEntity(path -> transformer(v))(unwrap(), protoPath, matchedProtoFields)(preserveProtoFieldNames)) {
                        logger.debug(s"Could not match stringified collection head ${z.toString} with field name ${jf._1} to schema of $clazz")
                      }
                    }

                    if (protoOpt.isEmpty)
                    // next, try to match the head field-by-field
                      verifyObjectResult(matchObjectAndEnrichEntity((path, z))((protoPath, unwrap()), runningResult._2)(preserveProtoFieldNames)) {
                        logger.debug(s"No fields of collection head $z matched schema of $clazz")
                        onArrayLeft()
                      } else onArrayLeft()
                  }
                }
                // collection of identical primitives
                case (true, None) => {
                  // try to match the head as a value with the same field name
                  verifyFieldResult(matchFieldAndEnrichEntity(path -> transformer(v))(unwrap(), protoPath, matchedProtoFields)(preserveProtoFieldNames)) {
                    // try to match the head as a string with the same field name
                    transformer = x => JString(JsonMethods.compact(x.asInstanceOf[JArray].arr.head))
                    logger.debug(s"Collection head field ${jf._1 -> v1} did not match schema of $clazz")
                    verifyFieldResult(matchFieldAndEnrichEntity(path -> transformer(v))(unwrap(), protoPath, matchedProtoFields)(preserveProtoFieldNames)) {
                      logger.debug(s"Could not match stringified collection head ${v1.toString} with field name ${jf._1} to schema of $clazz")
                    }
                  }
                }
                // collections of different objects
                // don't do anything
                case (false, _) => {
                  logger.debug(s"Could not match JSON collection ${v.arr} with field name ${jf._1} to schema of $clazz")
                }
              }
            }
          }
          // field is a Set
          case v: JSet if !v.set.isEmpty => {
            // fuzzy match on collections that do not match to Proto collections
            // try to match the whole collection as a string with the same field name: only works if the matching Proto field is a string
            transformer = x => JString(JsonMethods.compact(x))
            verifyFieldResult(matchFieldAndEnrichEntity(path -> transformer(v))(unwrap(), protoPath, matchedProtoFields)(preserveProtoFieldNames)) {
              logger.debug(s"Could not match stringified collection ${v.set.toString} with field name ${jf._1} to schema of $clazz")
              val v1 = v.set.head
              val allSame = v.set.foldLeft[Boolean](true)((b, z) => b && (v1 == z))
              transformer = x => x.asInstanceOf[JSet].set.head
              (allSame, nextObject(v1)) match {
                // collections of identical objects:
                case (true, Some(z)) => {
                  // first try to match the head as a whole object with the same field name
                  verifyFieldResult(matchFieldAndEnrichEntity(path -> transformer(v))(unwrap(), protoPath, matchedProtoFields)(preserveProtoFieldNames)) {
                    logger.debug(s"Collection head field ${jf._1 -> z} did not match schema of $clazz")

                    def onSetLeft(): Unit = {
                      // try to match the head as a string with the same field name
                      transformer = x => JString(JsonMethods.compact(x.asInstanceOf[JSet].set.head))
                      verifyFieldResult(matchFieldAndEnrichEntity(path -> transformer(v))(unwrap(), protoPath, matchedProtoFields)(preserveProtoFieldNames)) {
                        logger.debug(s"Could not match stringified collection head ${z.toString} with field name ${jf._1} to schema of $clazz")
                      }
                    }

                    if (protoOpt.isEmpty)
                    // next, try to match the head field-by-field
                      verifyObjectResult(matchObjectAndEnrichEntity((path, z))((protoPath, unwrap()), runningResult._2)(preserveProtoFieldNames)) {
                        logger.debug(s"No fields of collection head $z matched schema of $clazz")
                        onSetLeft()
                      } else onSetLeft()
                  }
                }
                // collection of identical primitives
                case (true, None) => {
                  // try to match the head as a value with the same field name
                  verifyFieldResult(matchFieldAndEnrichEntity(path -> transformer(v))(unwrap(), protoPath, matchedProtoFields)(preserveProtoFieldNames)) {
                    // try to match the head as a string with the same field name
                    transformer = x => JString(JsonMethods.compact(x.asInstanceOf[JSet].set.head))
                    logger.debug(s"Collection head field ${jf._1 -> v1} did not match schema of $clazz")
                    verifyFieldResult(matchFieldAndEnrichEntity(path -> transformer(v))(unwrap(), protoPath, matchedProtoFields)(preserveProtoFieldNames)) {
                      logger.debug(s"Could not match stringified collection head ${v1.toString} with field name ${jf._1} to schema of $clazz")
                    }
                  }
                }
                // collections of different objects
                // don't do anything
                case (false, _) => {
                  logger.debug(s"Could not match JSON collection ${v.set} with field name ${jf._1} to schema of $clazz")
                }
              }
            }
          }
          //field is a primitive which has no matching Proto field
          case v => {
            transformer = x => JArray(List(x))
            // try to match the value to a collection with the same field name
            verifyFieldResult(matchFieldAndEnrichEntity(path -> transformer(v))(unwrap(), protoPath,matchedProtoFields)(preserveProtoFieldNames)) {
              logger.debug(s"Could not match singleton collection ${v.toString} with field name ${jf._1} to schema of $clazz")
              // try to match the value as a string with the same field name
              transformer = x => JString(JsonMethods.compact(x))
              verifyFieldResult(matchFieldAndEnrichEntity(path -> transformer(v))(unwrap(), protoPath, matchedProtoFields)(preserveProtoFieldNames)) {
                logger.debug(s"Could not match stringified ${v.toString} with field name ${jf._1} to schema of $clazz")
               }
            }
          }
        }
      }})
    runningResult
  }

  def toANLC(s: String): String = s.replaceAll("[^a-zA-Z0-9]", "").toLowerCase


  private def isPerfectMatch(jsonField: JsonPath, protoFd: FieldDescriptor) = {

    def flattenJsonPath(p: JsonPath) = {
      p.foldLeft("")((st, seg) => st ++  toANLC(seg._2))
    }


    //the JSON field has a perfect match of its full JSON path to the full Proto name
    val jsonFieldName = jsonField.last._2
    val protoName = protoFd.name
    val pre = toANLC(protoName).replaceFirst(toANLC(jsonFieldName),"")
    if (pre.size < protoName.size) {
      (pre.isEmpty && jsonField.size == 1) || (!pre.isEmpty && flattenJsonPath(jsonField).contains(pre))
    } else false

  }

  /**
    * Recursively searches for a matching Proto field and updates the Proto, if matched
    */
  private def matchFieldAndEnrichEntity(jsonField: JsonField)(target: GeneratedMessage, prefix: ProtoPath, matchedProtoFields: Set[FieldDescriptor])(preserveProtoFieldName: PreserveProtoFieldNames): Either[(Option[Either[ProtoPath,ProtoPath]],GeneratedMessage), (ProtoPath,GeneratedMessage)] = {
    val clazz = target.getClass



    Try {
      val field: JField = (jsonField._1.last._2, jsonField._2)
      val scalaDescriptor = target.companion.scalaDescriptor
      val companion = target.companion
      var result: Either[(Option[Either[ProtoPath,ProtoPath]],GeneratedMessage), (ProtoPath, GeneratedMessage)] = Left((None,target))
      val fnextractor = getJsonFieldNameExtractor(getPreserveFieldNamesFlag(companion, preserveProtoFieldName).flag)
      val originalMatchingFields = scalaDescriptor.fields.filter(fd =>
        // exact JSON field name match
        fnextractor.apply(fd).contains(field._1))
      var matchingFields = originalMatchingFields.filter(isPerfectMatch(jsonField._1,_))
      if (matchingFields.isEmpty) {
          matchingFields = originalMatchingFields.filter(fd =>
            // this Proto field has not been matched yet
            !matchedProtoFields.contains(fd))
          if (matchingFields.isEmpty && !originalMatchingFields.isEmpty){
            val f = originalMatchingFields.head
            // this JSON field is still a match - but all matching Proto fields have been taken
            result = Left(Some(Left(prefix :+ f)), target)
            // try to convert to check if type fits

            convertJsonFieldValueToProto(field._2, (f, companion.asInstanceOf[C]))(getPreserveFieldNamesFlag(companion, preserveProtoFieldName)).foreach(newFieldValue => {
              // field matches but is not a perfect match
              logger.debug(s"Field $field is not the best match but it matched schema of ${clazz} with descriptor $f and value $newFieldValue")
              result = Left(Some(Right(prefix :+ f)), target)
            })
          }
      }

      //first attempt: direct match on JSON field names and coarse types
      matchingFields.headOption.foreach(f => {
        result = Left(Some(Left(prefix :+ f)), target)
        convertJsonFieldValueToProto(field._2, (f, companion.asInstanceOf[C]))(getPreserveFieldNamesFlag(companion, preserveProtoFieldName)).foreach(newFieldValue => {
          logger.debug(s"Field $field matched schema of ${clazz} with descriptor $f and value $newFieldValue")
          result = Right((prefix :+ f) ->
            innerMutateProtoEntity(target, f, newFieldValue))
        })})
      // Second attempt: search nested fields and apply recursively
      if (result.isLeft && result.left.get._1.isEmpty) {
        logger.debug(s"Field $field did not match any direct schema fields of $clazz")
        scalaDescriptor.fields.foreach(f => {
          if (result.isLeft && result.left.get._1.isEmpty) {
            safeGetFieldCompanion(companion, f.number) match {
              case Some(x) => {
                val childValue = target.getField(f) match {
                  case v: PMessage => v.as(x.asInstanceOf[C].messageReads)
                  case z: PRepeated => z.value.lastOption.map(_.as(x.asInstanceOf[C].messageReads)).getOrElse(x.asInstanceOf[C].defaultInstance)
                  case _ => x.asInstanceOf[C].defaultInstance
                }
                val newChildValue = matchFieldAndEnrichEntity(jsonField)(childValue, prefix :+ f, matchedProtoFields)(getPreserveFieldNamesFlag(x, preserveProtoFieldName))
                if (newChildValue.isRight) {
                  val newFieldValue = newChildValue.right.get
                  logger.debug(s"Field $field matched schema of $clazz with descriptor $f and value ${newFieldValue._2}")
                  result = Right(newFieldValue._1 -> innerMutateProtoEntity(target, f, newFieldValue._2.toPMessage))
                } else {
                  logger.debug(s"Field $field is not the best match for schema of $clazz")
                  result = Left(newChildValue.left.get._1,target)
                }
              }
              case _ => {}
            }
          }
        })
      }
      result
    }.recoverWith { case exc: Throwable => {
      logger.warn(s"Caught $exc while parsing JSON $jsonField for Proto clazz $clazz")
      Failure(exc)
    }}.getOrElse(Left((None,target)))
  }


}

object ProtobufJsonImplicits {

  implicit def toJavaBiFunction[X,Y,Z](f: Function2[X,Y,Z]): BiFunction[X,Y,Z] = new BiFunction[X,Y,Z]{

    override def apply(x: X, y: Y): Z = f(x,y)

    override def andThen[V](after: function.Function[_ >: Z, _ <: V]): BiFunction[X, Y, V] =
      toJavaBiFunction((x:X,y:Y) => after.apply(f(x,y)))
  }

  implicit def toJavaBiOperator[X](f: Function2[X,X,X]) = new BinaryOperator[X]{

    override def apply(x: X, y: X): X = f(x,y)

    override def andThen[V](after: function.Function[_ >: X, _ <: V]): BiFunction[X, X, V] =
      toJavaBiFunction((x:X,y:X) => after.apply(f(x,y)))
  }
}
