package com.omeregeozkaya.boun;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

@Data
@NoArgsConstructor
@DefaultSchema(JavaBeanSchema.class)
public class JsonMessage {
    private String timestamp;
    private String device;
    private String temperature;
}