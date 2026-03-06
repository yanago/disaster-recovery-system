package com.disasterrecovery.api.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public final class ErrorResponse {

    private final String message;

    public ErrorResponse(String message) {
        this.message = message;
    }

    @JsonProperty("message")
    public String getMessage() {
        return message;
    }
}
