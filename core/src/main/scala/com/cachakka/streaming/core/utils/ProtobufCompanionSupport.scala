package com.cachakka.streaming.core.utils


import com.trueaccord.scalapb.{GeneratedMessage, GeneratedMessageCompanion, Message}
import com.typesafe.scalalogging.LazyLogging
import com.cachakka.streaming.core.api.{StreamingEntity, StreamingEntityCompanion}

import scala.util.Try

object ProtobufCompanionSupport extends LazyLogging{


  /**
    * Finds the streaming companion for a given streaming entity class
    * @param cl
    * @tparam E
    * @return
    */
  def findStreamingEntityCompanion[E<:StreamingEntity[_,E]](implicit cl:Class[E]): StreamingEntityCompanion[_,E] = {
    findCompanion[E](cl).asInstanceOf[StreamingEntityCompanion[_,E]]
  }


  def findCompanion[E<:GeneratedMessage with Message[E]](implicit cl: Class[E]): GeneratedMessageCompanion[E] = {
     findAnyCompanion(cl).get.asInstanceOf[GeneratedMessageCompanion[E]]
  }

  private def findAnyCompanion(cl: Class[_]) = {
    /*import scala.reflect.runtime.universe._

     val mirror = runtimeMirror(cl.getClassLoader)
     val companionSymbol = rootMirror.classSymbol(cl).companion.asModule
     mirror.reflectModule(companionSymbol).instance.asInstanceOf[StreamingEntityCompanion[_,E]]*/
    Try{val companionClass = Class.forName(cl.getName + "$", true, cl.getClassLoader)
    val module = companionClass.getField("MODULE$")
    module.get(companionClass)}
  }


}
