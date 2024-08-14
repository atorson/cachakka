package com.cachakka.streaming.core.utils

import java.lang.reflect.ParameterizedType

import com.google.common.reflect.TypeToken
import scala.util.Try

object ReflectionUtils {

    def inferTypeParameter[U, C<:U](interfaceClazz: Class[U], instance: C, index: Int): Try[Class[_]]= Try {
       val token = TypeToken.of(instance.getClass.asInstanceOf[Class[C]])
       val superToken = token.getSupertype(interfaceClazz).getType.asInstanceOf[ParameterizedType]
       TypeToken.of(superToken.getActualTypeArguments.apply(index)).getRawType
    }
}
