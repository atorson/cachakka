package com.cachakka.streaming

import scala.util.Try

package object akka {

  def findCompanion[T](cl: Class[_]): Try[T] = findCompanion[T](cl.getName, cl.getClassLoader)

  def findCompanion[T](clazzFQN: String, classLoader: ClassLoader): Try[T] = {
    Try{val companionClass = Class.forName(clazzFQN + "$", true, classLoader)
      val module = companionClass.getField("MODULE$")
      module.get(companionClass).asInstanceOf[T]}
  }
}
