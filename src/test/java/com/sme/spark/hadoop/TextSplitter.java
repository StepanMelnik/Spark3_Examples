package com.sme.spark.hadoop;

import static org.apache.commons.lang3.StringUtils.isEmpty;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

/**
 * Useful utilities to split text.
 */
final class TextSplitter
{
    private static final Logger LOGGER = LoggerFactory.getLogger(TextSplitter.class);

    private static final Pattern ALPHABETIC_PATTERN = Pattern.compile("[a-zA-Z]*");
    private static final Pattern WORDS_WITH_SPACE_PATTERN = Pattern.compile("(\\s*)(\\S+)");

    // private constructor
    private TextSplitter()
    {
    }

    /**
     * Split text by alphabetic pattern.
     * 
     * @param text The given text;
     * @return Returns alphabetic value if exists otherwise empty value.
     */
    static String splitAphaBetic(final String text)
    {
        Matcher matcher = ALPHABETIC_PATTERN.matcher(text);
        return matcher.find() ? matcher.group() : "";
    }

    /**
     * Split the given text with space + word pattern.
     * 
     * @param text The given text;
     * @return Return immutable list of space + word groups.
     */
    static ImmutableList<ImmutablePair<String, String>> splitWithSpaces(final String text)
    {
        ImmutableList.Builder<ImmutablePair<String, String>> builder = new ImmutableList.Builder<>();

        Matcher matcher = WORDS_WITH_SPACE_PATTERN.matcher(text);
        while (matcher.find())
        {
            String group0 = matcher.group(0);
            String group1 = matcher.group(1);

            if (isEmpty(group0) && isEmpty(group1))
            {
                LOGGER.trace("Skip {} group", matcher.group());
            }
            else
            {
                builder.add(ImmutablePair.of(group0, group1));
            }
        }
        return builder.build();
    }
}
