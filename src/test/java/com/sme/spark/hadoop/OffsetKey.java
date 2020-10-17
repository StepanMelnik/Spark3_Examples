package com.sme.spark.hadoop;

import static org.apache.commons.lang3.builder.EqualsBuilder.reflectionEquals;
import static org.apache.commons.lang3.builder.HashCodeBuilder.reflectionHashCode;
import static org.apache.commons.lang3.builder.ToStringBuilder.reflectionToString;

import java.io.Serializable;

import org.apache.commons.lang3.builder.ToStringStyle;

/**
 * Provides domain object to work with offset data per key. Also Hive needs default constructor and all get/set properties.
 */
public final class OffsetKey implements Serializable
{
    private String key;
    private long lineOffset;
    private long charOffset;

    public OffsetKey()
    {
    }

    public OffsetKey(final String key, final long lineOffset, final long charOffset)
    {
        this.key = key;
        this.lineOffset = lineOffset;
        this.charOffset = charOffset;
    }

    public String getKey()
    {
        return key;
    }

    public void setKey(String key)
    {
        this.key = key;
    }

    public long getLineOffset()
    {
        return lineOffset;
    }

    public void setLineOffset(long lineOffset)
    {
        this.lineOffset = lineOffset;
    }

    public long getCharOffset()
    {
        return charOffset;
    }

    public void setCharOffset(long charOffset)
    {
        this.charOffset = charOffset;
    }

    @Override
    public int hashCode()
    {
        return reflectionHashCode(this);
    }

    @Override
    public boolean equals(Object obj)
    {
        return reflectionEquals(this, obj);
    }

    @Override
    public String toString()
    {
        return reflectionToString(this, ToStringStyle.JSON_STYLE);
    }
}
