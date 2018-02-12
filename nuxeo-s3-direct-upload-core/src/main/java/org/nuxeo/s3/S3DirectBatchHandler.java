package org.nuxeo.s3;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest;
import com.amazonaws.services.securitytoken.model.AssumeRoleResult;
import lombok.extern.apachecommons.CommonsLog;
import org.apache.commons.lang3.StringUtils;
import org.nuxeo.ecm.automation.server.jaxrs.batch.Batch;
import org.nuxeo.ecm.automation.server.jaxrs.batch.BatchFileEntry;
import org.nuxeo.ecm.automation.server.jaxrs.batch.handler.AbstractBatchHandler;
import org.nuxeo.ecm.automation.server.jaxrs.batch.handler.BatchFileInfo;
import org.nuxeo.ecm.core.api.Blob;
import org.nuxeo.ecm.core.api.NuxeoException;
import org.nuxeo.ecm.core.blob.BlobInfo;
import org.nuxeo.ecm.core.blob.ManagedBlob;
import org.nuxeo.ecm.core.blob.SimpleManagedBlob;
import org.nuxeo.ecm.core.blob.binary.Binary;
import org.nuxeo.ecm.core.blob.binary.BinaryBlob;
import org.nuxeo.ecm.core.blob.binary.CachingBinaryManager;
import org.nuxeo.ecm.core.blob.binary.LazyBinary;
import org.nuxeo.ecm.core.transientstore.api.TransientStore;
import org.nuxeo.ecm.core.transientstore.api.TransientStoreService;
import org.nuxeo.ecm.platform.audit.impl.ExtendedInfoImpl;
import org.nuxeo.runtime.api.Framework;
import org.nuxeo.s3.blob.S3BlobInfo;
import org.nuxeo.s3.blob.S3RefBlob;

import java.io.Serializable;
import java.text.MessageFormat;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

@CommonsLog public class S3DirectBatchHandler extends AbstractBatchHandler {

    public static final String DEFAULT_S3_HANDLER_NAME = "s3direct";

    private String transientStoreName;

    private TransientStore transientStore;

    private AWSCredentials awsCredentials;

    private AWSSecurityTokenService client;
    private AmazonS3 s3Client;

    private String roleArnToAssume;
    private String awsRegion;
    private boolean s3AccelerationSupported;
    private String bucket;
    private String baseBucketKey;

    public S3DirectBatchHandler() {
        super(DEFAULT_S3_HANDLER_NAME);
    }

    @Override public Batch newBatch() {
        return initBatch();
    }

    @Override public Batch getBatch(String batchId) {
        TransientStore transientStore = getTransientStore();
        Map<String, Serializable> batchEntryParams = transientStore.getParameters(batchId);

        if (batchEntryParams == null) {
            if (!hasBatch(batchId)) {
                return null;
            }
            batchEntryParams = new HashMap<>();
        }

        String batchProvider = batchEntryParams.getOrDefault("provider", getName()).toString();
        batchEntryParams.remove("provider");

        if (!getName().equalsIgnoreCase(batchProvider)) {
            return null;
        }

        Batch batch = new Batch(transientStore, getName(), batchId, batchEntryParams, this);

        Map<String, Object> batchExtraInfo = batch.getBatchExtraInfo();
        AssumeRoleResult assumeRoleResult = client.assumeRole(
                new AssumeRoleRequest().withRoleSessionName(batch.getKey()).withRoleArn(roleArnToAssume));
        batchExtraInfo.put("awsSecretKeyId", assumeRoleResult.getCredentials().getAccessKeyId());
        batchExtraInfo.put("awsSecretAccessKey", assumeRoleResult.getCredentials().getSecretAccessKey());
        batchExtraInfo.put("awsSessionToken", assumeRoleResult.getCredentials().getSessionToken());
        batchExtraInfo.put("bucket", bucket);
        batchExtraInfo.put("baseKey", baseBucketKey);
        batchExtraInfo.put("expiration", assumeRoleResult.getCredentials().getExpiration().toInstant().toEpochMilli());
        batchExtraInfo.put("region", awsRegion);
        batchExtraInfo.put("useS3Accelerate", s3AccelerationSupported);

        return batch;
    }

    private boolean hasBatch(String batchId) {
        return !StringUtils.isEmpty(batchId) && transientStore.exists(batchId);
    }

    @Override public Batch newBatch(String batchId) {
        Batch batch = initBatch(batchId);

        return initBatch(batchId);
    }

    @Override public void init(Map<String, String> configProperties) {
        if (!containsRequired(configProperties)) {
            throw new NuxeoException();
        }

        transientStoreName = configProperties.get(Constants.Config.Properties.TRANSIENT_STORE_NAME);

        String awsSecretKeyId = configProperties.get(Constants.Config.Properties.AWS_SECRET_KEY_ID);
        String awsSecretAccessKey = configProperties.get(Constants.Config.Properties.AWS_SECRET_ACCESS_KEY);
        awsRegion = configProperties.get(Constants.Config.Properties.AWS_REGION);
        roleArnToAssume = configProperties.get(Constants.Config.Properties.AWS_ROLE_ARN);
        bucket = configProperties.get(Constants.Config.Properties.AWS_BUCKET);
        baseBucketKey = configProperties.getOrDefault(Constants.Config.Properties.AWS_BUCKET_BASE_KEY, "/");

        s3AccelerationSupported =
            Boolean.parseBoolean(
                configProperties.getOrDefault(Constants.Config.Properties.USE_S3_ACCELERATION, Boolean.FALSE.toString())
            );

        client = initClient(awsSecretKeyId, awsSecretAccessKey, awsRegion);
        s3Client = initS3Client(awsSecretKeyId, awsSecretAccessKey, awsRegion, s3AccelerationSupported);

        super.init(configProperties);
    }

    @Override
    public boolean completeUpload(String batchId, String fileIndex, BatchFileInfo fileInfo) {
        Batch batch = getBatch(batchId);

        ObjectMetadata s3ClientObjectMetadata = s3Client.getObjectMetadata(bucket, fileInfo.getKey());

        if (s3ClientObjectMetadata == null) {
            return false;
        }

        BlobInfo blobInfo = new BlobInfo();
        blobInfo.key = s3ClientObjectMetadata.getETag();
        blobInfo.filename = fileInfo.getName();
        blobInfo.length = s3ClientObjectMetadata.getContentLength();
        blobInfo.digest = fileInfo.getKey();
        blobInfo.mimeType = s3ClientObjectMetadata.getContentType();

        Blob blob = new BinaryBlob(
                new LazyBinary(blobInfo.key, transientStoreName, null)
                , blobInfo.key
                , blobInfo.filename
                , blobInfo.mimeType
                , blobInfo.encoding
                , blobInfo.digest
                , blobInfo.length
        );

        batch.addBlob(fileIndex, blob);

        return true;
    }

    private boolean containsRequired(Map<String, String> configProperties) {
        return configProperties.containsKey(Constants.Config.Properties.AWS_SECRET_KEY_ID)
                && configProperties.containsKey(Constants.Config.Properties.AWS_SECRET_ACCESS_KEY)
                && configProperties.containsKey(Constants.Config.Properties.AWS_ROLE_ARN)
                && configProperties.containsKey(Constants.Config.Properties.AWS_REGION)
                && configProperties.containsKey(Constants.Config.Properties.AWS_BUCKET)
                && configProperties.containsKey(Constants.Config.Properties.TRANSIENT_STORE_NAME);
    }

    @Override protected TransientStore getTransientStore() {
        if (transientStore == null) {
            TransientStoreService service = Framework.getService(TransientStoreService.class);
            transientStore = service.getStore(transientStoreName);
        }

        return transientStore;
    }

    private AWSSecurityTokenService initClient(String awsSecretKeyId, String awsSecretAccessKey, String region) {
        return AWSSecurityTokenServiceClientBuilder
                .standard()
                .withRegion(region)
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(awsSecretKeyId, awsSecretAccessKey)))
                .build()
        ;
    }

    private AmazonS3 initS3Client(String awsSecretKeyId, String awsSecretAccessKey, String region, boolean useAccelerate) {
        return AmazonS3ClientBuilder
                .standard()
                .withRegion(region)
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(awsSecretKeyId, awsSecretAccessKey)))
                .withAccelerateModeEnabled(useAccelerate)
                .build()
                ;
    }
}
