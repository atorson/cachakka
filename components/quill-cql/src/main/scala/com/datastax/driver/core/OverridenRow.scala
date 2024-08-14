package com.datastax.driver.core

import java.math.BigInteger
import java.net.InetAddress
import java.nio.ByteBuffer
import java.util
import java.util.{Date, UUID}

import com.google.common.reflect.TypeToken

class OverridenRow(val delegate: Row) extends AbstractGettableData(ProtocolVersion.V4) with Row {
  override def getIndexOf(name: String): Int = delegate.getColumnDefinitions.getFirstIdx(name);

  override def getPartitionKeyToken: Token = delegate.getPartitionKeyToken

  override def getColumnDefinitions: ColumnDefinitions = delegate.getColumnDefinitions

  override def getToken(i: Int): Token = delegate.getToken(i)

  override def getToken(name: String): Token = delegate.getToken(name)

  override def getName(i: Int): String = delegate.getColumnDefinitions.getName(i)

  override def getValue(i: Int): ByteBuffer = null

  override def getCodecRegistry: CodecRegistry = delegate.getColumnDefinitions.codecRegistry

  override def getType(i: Int): DataType = delegate.getColumnDefinitions.getType(i)

  override def getByte(i: Int): Byte = delegate.getByte(i)

  override def getTime(i: Int): Long = delegate.getTime(i)

  override def getTupleValue(i: Int): TupleValue = delegate.getTupleValue(i)

  override def getDouble(i: Int): Double = delegate.getDouble(i)

  override def getInet(i: Int): InetAddress = delegate.getInet(i)

  override def getFloat(i: Int): Float = delegate.getFloat(i)

  override def getUDTValue(i: Int): UDTValue = {
    val x = delegate.getUDTValue(i)
    new OverridenUdtValue(x)
  }

  override def getBytes(i: Int): ByteBuffer = delegate.getBytes(i)

  override def getUUID(i: Int): UUID = delegate.getUUID(i)

  override def getBytesUnsafe(i: Int): ByteBuffer = delegate.getBytesUnsafe(i)

  override def getTimestamp(i: Int): Date = delegate.getTimestamp(i)

  override def getList[T](i: Int, elementsClass: Class[T]): util.List[T] = delegate.getList(i, elementsClass)

  override def getList[T](i: Int, elementsType: TypeToken[T]): util.List[T] = delegate.getList(i, elementsType)

  override def get[T](i: Int, targetClass: Class[T]): T = delegate.get(i, targetClass)

  override def get[T](i: Int, targetType: TypeToken[T]): T = delegate.get(i, targetType)

  override def get[T](i: Int, codec: TypeCodec[T]): T = delegate.get(i, codec)

  override def getDate(i: Int): LocalDate = delegate.getDate(i)

  override def getBool(i: Int): Boolean = delegate.getBool(i)

  override def getDecimal(i: Int): java.math.BigDecimal = delegate.getDecimal(i)

  override def getVarint(i: Int): BigInteger = delegate.getVarint(i)

  override def getObject(i: Int): AnyRef = delegate.getObject(i)

  override def getSet[T](i: Int, elementsClass: Class[T]): util.Set[T] = delegate.getSet(i, elementsClass)

  override def getSet[T](i: Int, elementsType: TypeToken[T]): util.Set[T] = delegate.getSet(i, elementsType)

  override def getShort(i: Int): Short = delegate.getShort(i)

  override def getString(i: Int): String = delegate.getString(i)

  override def getMap[K, V](i: Int, keysClass: Class[K], valuesClass: Class[V]): util.Map[K, V] = delegate.getMap(i, keysClass, valuesClass)

  override def getMap[K, V](i: Int, keysType: TypeToken[K], valuesType: TypeToken[V]): util.Map[K, V] = delegate.getMap(i, keysType, valuesType)

  override def getLong(i: Int): Long = delegate.getLong(i)

  override def getInt(i: Int): Int = delegate.getInt(i)

  override def isNull(i: Int): Boolean = delegate.isNull(i)
}
