package com.cachakka.streaming.core.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.protobuf.Message;
import com.trueaccord.scalapb.GeneratedMessageCompanion;
import com.trueaccord.scalapb.JavaProtoSupport;
import com.cachakka.streaming.core.utils.ProtobufCompanionSupport;
import com.cachakka.streaming.core.utils.ProtobufJsonSupport;
import com.cachakka.streaming.core.utils.StreamingProtoToJsonConfig;

public interface StreamingEntityCompanion<J extends Message, E extends StreamingEntity<J,E>> extends GeneratedMessageCompanion<E>,JavaProtoSupport<E,J>, StreamingProtoToJsonConfig {


    static <E extends StreamingEntity<?,E>> StreamingEntityCompanion<?,E> of(Class<E> clazz){
        return ProtobufCompanionSupport.findStreamingEntityCompanion(clazz);
    }

    /**
     * Unmarshalls Proto streaming entity from a JSON string
     * @param jsonString JSON string
     * @return streaming entity
     */
    default E fromJsonString(String jsonString) {return fromJsonString(jsonString, true);}

    /**
     * Unmarshalls Proto streaming entity from a JSON string
     * @param jsonString JSON string
     * @param exactMatchFirst if false, tries inexact match first
     * @return streaming entity
     */
    default E fromJsonString(String jsonString, boolean exactMatchFirst) {return ProtobufJsonSupport.fromJsonString(jsonString, exactMatchFirst, true, this);}


    /**
     * Unmarshalls Proto streaming entity from a JSON object
     * @param jsonObject JSON object
     * @return streaming entity
     */
    default E fromJsonNode(JsonNode jsonObject) {return ProtobufJsonSupport.fromJsonNode(jsonObject, true, true, this);}


}
