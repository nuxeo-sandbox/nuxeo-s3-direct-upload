package org.nuxeo.s3;

import lombok.Builder;
import lombok.Data;

@Data @Builder public class AWSS3Config {
    private String secretKeyId;

    private String secretAccessKey;

    private String bucketName;

    private String bucketRegion;

    private boolean transferAcceleration;
}