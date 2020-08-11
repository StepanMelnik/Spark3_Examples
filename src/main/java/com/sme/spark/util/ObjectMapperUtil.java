package com.sme.spark.util;

import java.util.Objects;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;

/**
 * The Utility to work with json object.
 */
public final class ObjectMapperUtil
{
    private static final Logger LOGGER = LogManager.getLogger(ObjectMapperUtil.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    static
    {
        OBJECT_MAPPER.setVisibility(OBJECT_MAPPER
                .getSerializationConfig()
                .getDefaultVisibilityChecker()
                .with(Visibility.NONE)
                .withFieldVisibility(Visibility.ANY))
                .registerModule(new ParameterNamesModule())
                .registerModule(new Jdk8Module())
                .registerModule(new JavaTimeModule());
        OBJECT_MAPPER.configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true);
    }

    // private class
    private ObjectMapperUtil()
    {
    }

    /**
     * Serialize a given instance to string value.
     * 
     * @param <T> Given instance to serialize as string value;
     * @param data The instance to be serialized as string value;
     * @return Returns serialized string value.
     */
    public static <T> String serialize(T data)
    {
        Objects.requireNonNull(data);
        try
        {
            return OBJECT_MAPPER.writeValueAsString(data);
        }
        catch (JsonProcessingException e)
        {
            LOGGER.error("Cannot serialize data: " + data, e);  // never must happen
            return "";
        }
    }

    /**
     * Deserialize a string value to the given type.
     * 
     * @param <T> Instance type;
     * @param resultClass The type to deserialize data;
     * @param data String value;
     * @return Returns created instance.
     */
    public static <T> T deserialize(Class<T> resultClass, String data)
    {
        try
        {
            return OBJECT_MAPPER.readValue(data, resultClass);
        }
        catch (Exception e)
        {
            throw new RuntimeException("Cannot deserizlize data: " + data);
        }
    }

    /**
     * Converts given value into instance of given value type.
     * 
     * @param <T> Instance type;
     * @param fromValue The given instance;
     * @param toType Output instance;
     * @return Returns converted instance.
     */
    public static <T> T convert(Object fromValue, Class<T> toType)
    {
        return OBJECT_MAPPER.convertValue(fromValue, toType);
    }
}
