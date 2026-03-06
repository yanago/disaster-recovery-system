package com.disasterrecovery.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

public final class Json {

    private Json() {
    }

    private static final ObjectMapper MAPPER = create();

    private static ObjectMapper create() {
        ObjectMapper om = new ObjectMapper();
        om.registerModule(new JavaTimeModule());
        om.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        return om;
    }

    public static ObjectMapper mapper() {
        return MAPPER;
    }
}

