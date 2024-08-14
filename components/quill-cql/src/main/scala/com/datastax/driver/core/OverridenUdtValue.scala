package com.datastax.driver.core

import java.lang.reflect.ParameterizedType

import com.google.common.reflect.TypeToken


object OverridenUdtValue {
  private val rawMapClazz = classOf[java.util.Map[_,_]]
  private val rawListClazz = classOf[java.util.List[_]]
}

class OverridenUdtValue(meta: UserType) extends UDTValue(meta) {

  def this(delegate:UDTValue) = {
    this(delegate.getType)
    Range(0,delegate.getType.byIdx.size).foreach(x => setValue(x, delegate.getValue(x)))
  }

  override def get[T](name: String, targetClass: Class[T]): T = {
    if (isNull(name)) null.asInstanceOf[T] else {
      if ((OverridenUdtValue.rawMapClazz.isAssignableFrom(targetClass) || OverridenUdtValue.rawListClazz.isAssignableFrom(targetClass))
          && !TypeToken.of(targetClass).getType.isInstanceOf[ParameterizedType]) {
        super.get(name, null.asInstanceOf[TypeToken[T]])
      } else {
        super.get(name, targetClass)
      }
    }
  }

  override def set[T](name: String, value: T, targetClass: Class[T]): UDTValue = {
    if (value == null) super.setToNull(name) else
       if ((OverridenUdtValue.rawMapClazz.isAssignableFrom(targetClass) || OverridenUdtValue.rawListClazz.isAssignableFrom(targetClass))
          && !TypeToken.of(targetClass).getType.isInstanceOf[ParameterizedType]) {
          super.set(name, value, null.asInstanceOf[TypeToken[T]])
        } else {
          super.set(name, value, targetClass)
        }

  }

}
