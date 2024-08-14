package com.cachakka.streaming.core.api;


import com.fasterxml.jackson.databind.JsonNode;
import com.google.protobuf.Message;
import com.trueaccord.scalapb.GeneratedMessage;
import com.cachakka.streaming.core.utils.ProtobufJsonSupport;
import org.joda.time.DateTime;


public interface StreamingEntity<J extends Message, E extends StreamingEntity<J,E>> extends GeneratedMessage, com.trueaccord.scalapb.Message<E>{

    default E reifySelf(){
        return (E) this;
    }

    default StreamingEntityCompanion<J,E> getCompanion(){
        return (StreamingEntityCompanion<J,E>) companion();
    }

    default J toJava(){
        return getCompanion().toJavaProto(reifySelf());
    }

    default String toJsonString() {return ProtobufJsonSupport.toJsonString(reifySelf(), true);}

    default JsonNode toJsonNode() {return ProtobufJsonSupport.toJsonNode(reifySelf());}

}
