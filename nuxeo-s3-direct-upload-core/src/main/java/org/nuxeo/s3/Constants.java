package org.nuxeo.s3;

import lombok.experimental.UtilityClass;

@UtilityClass
public class Constants {

    public static class Config {
        public static class Properties {
            public static final String PROVIDER_NAME = "name";
            public static final String TRANSIENT_STORE_NAME = "transientStore";
            public static final String AWS_SECRET_KEY_ID = "awsSecretKeyId";
            public static final String AWS_SECRET_ACCESS_KEY = "awsSecretAccessKey";
            public static final String AWS_REGION = "awsRegion";
            public static final String AWS_ROLE_ARN = "roleArn";
            public static final String USE_S3_ACCELERATION = "useS3Acceleration";

            public static final String AWS_BUCKET = "awsBucket";
            public static final String AWS_BUCKET_BASE_KEY = "baseKey";
        }
    }
}
