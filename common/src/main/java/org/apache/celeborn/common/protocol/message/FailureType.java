package org.apache.celeborn.common.protocol.message;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public enum FailureType {
    PUSH_FAILED(0, "PushFailed", "ShuffleWrite"),
    FETCH_FAILED(1, "FetchFailed", "ShuffleRead"),
    REVIVE_FAILED(2, "ReviveFailed", "ShuffleWrite"),
    CLEANUP(3, "CleanUp", "ShuffleWrite"),
    PUSH_LIMIT_FAILED(4, "PushLimitFailed", "ShuffleWrite"),
    REGISTER_SHUFFLE_FAILED(5, "RegisterShuffleFailed", "ShuffleWrite"),
    GET_PARTITION_FAILED(6, "GetPartitionFailed", "ShuffleRead"),
    MAPPER_END_FAILED(7, "MapperEndFailed", "ShuffleWrite"),
    UNKNOWN(15, "Unknown", "Other");

    private final int value;
    private final String display;
    private final String category;

    FailureType(int value, String display, String category) {
        this.value = value;
        this.display = display;
        this.category = category;
    }

    public final int getValue() {
        return value;
    }

    public final String getDisplay() {
        return display;
    }

    public String getCategory() {
        return category;
    }

    private static final Map<Integer, FailureType> lookup =
            Arrays.stream(FailureType.values()).collect(Collectors.toMap(FailureType::getValue, i -> i));

    public static FailureType fromValue(int value) {
        FailureType code = lookup.get(value);
        if (code != null) {
            return code;
        }
        throw new IllegalArgumentException("Unknown status code: " + value);
    }

}
