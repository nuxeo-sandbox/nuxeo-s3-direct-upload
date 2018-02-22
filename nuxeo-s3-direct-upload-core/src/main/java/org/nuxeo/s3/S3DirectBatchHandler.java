package org.nuxeo.s3;

import static org.apache.commons.lang3.StringUtils.isBlank;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.CopyObjectResult;
import com.amazonaws.services.s3.model.CopyPartRequest;
import com.amazonaws.services.s3.model.CopyPartResult;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest;
import com.amazonaws.services.securitytoken.model.AssumeRoleResult;

import lombok.extern.apachecommons.CommonsLog;

import org.apache.commons.lang3.StringUtils;
import org.nuxeo.ecm.automation.server.jaxrs.batch.Batch;
import org.nuxeo.ecm.automation.server.jaxrs.batch.handler.AbstractBatchHandler;
import org.nuxeo.ecm.automation.server.jaxrs.batch.handler.BatchFileInfo;
import org.nuxeo.ecm.core.api.Blob;
import org.nuxeo.ecm.core.api.NuxeoException;
import org.nuxeo.ecm.core.blob.BlobInfo;
import org.nuxeo.ecm.core.blob.binary.BinaryBlob;
import org.nuxeo.ecm.core.blob.binary.LazyBinary;
import org.nuxeo.ecm.core.transientstore.api.TransientStore;
import org.nuxeo.ecm.core.transientstore.api.TransientStoreService;
import org.nuxeo.runtime.api.Framework;

import java.io.IOException;
import java.io.Serializable;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@CommonsLog
public class S3DirectBatchHandler extends AbstractBatchHandler {

    public static final String DEFAULT_S3_HANDLER_NAME = "s3direct";

    private static final Pattern REGEX_MULTIPART_ETAG = Pattern.compile("-\\d+$");

    private static final long FIVE_GB = 5_368_709_120L;

    private String transientStoreName;

    private TransientStore transientStore;

    private AWSSecurityTokenService stsClient;
    private AmazonS3 s3Client;

    private String roleArnToAssume;
    private String awsRegion;
    private boolean s3AccelerationSupported;
    private String bucket;
    private String baseBucketKey;

    public S3DirectBatchHandler() {
        super(DEFAULT_S3_HANDLER_NAME);
    }

    @Override
    public Batch newBatch() {
        return initBatch();
    }

    @Override
    public Batch getBatch(String batchId) {
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
        AssumeRoleResult assumeRoleResult = stsClient.assumeRole(
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

    @Override
    public Batch newBatch(String batchId) {
        return initBatch(batchId);
    }

    @Override
    protected void init(Map<String, String> configProperties) {
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

        stsClient = initClient(awsSecretKeyId, awsSecretAccessKey, awsRegion);
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

        String etag = s3ClientObjectMetadata.getETag();
        String mimeType = s3ClientObjectMetadata.getContentType();

        if (StringUtils.isEmpty(etag)) {
            return false;
        }

        boolean isMultipartUpload = REGEX_MULTIPART_ETAG.matcher(etag).find();

        ObjectMetadata updatedObjectMetadata;

        if (s3ClientObjectMetadata.getContentLength() > FIVE_GB) {
            updatedObjectMetadata = copyBigFile(s3ClientObjectMetadata, bucket, fileInfo.getKey(), etag, true);
        } else {
            updatedObjectMetadata = copyFile(s3ClientObjectMetadata, bucket, fileInfo.getKey(), etag, true);
            if (isMultipartUpload) {
                String previousEtag = etag;
                etag = updatedObjectMetadata.getETag();
                updatedObjectMetadata = copyFile(s3ClientObjectMetadata, bucket, previousEtag, etag, true);
            }
        }

        BlobInfo blobInfo = new BlobInfo();
        blobInfo.key = MessageFormat.format("{0}:{1}", transientStoreName, etag);
        blobInfo.filename = fileInfo.getName();
        blobInfo.length = updatedObjectMetadata.getContentLength();
        blobInfo.digest = updatedObjectMetadata.getContentMD5();
        blobInfo.mimeType = mimeType;

        Blob blob = new BinaryBlob(
                new LazyBinary(blobInfo.key, transientStoreName, null)
                , blobInfo.key
                , blobInfo.filename
                , blobInfo.mimeType
                , blobInfo.encoding
                , blobInfo.digest
                , blobInfo.length
        );

        try {
            batch.addFile(fileIndex, blob, blobInfo.filename, blobInfo.mimeType);
        } catch (Exception e) {
            throw new NuxeoException(e);
        }

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

    @Override
    protected TransientStore getTransientStore() {
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

    protected AmazonS3 initS3Client(String awsSecretKeyId, String awsSecretAccessKey, String region, boolean useAccelerate) {
        AWSCredentialsProvider awsCredentialsProvider = getAwsCredentialsProvider(awsSecretKeyId, awsSecretAccessKey);
        return AmazonS3ClientBuilder
                .standard()
                .withRegion(region)
                .withCredentials(awsCredentialsProvider)
                .withAccelerateModeEnabled(useAccelerate)
                .build()
                ;
    }

    protected AWSCredentialsProvider getAwsCredentialsProvider(String awsSecretKeyId, String awsSecretAccessKey) {
        AWSCredentialsProvider result;
        if (isBlank(awsSecretKeyId) || isBlank(awsSecretAccessKey)) {
            result = InstanceProfileCredentialsProvider.getInstance();
            try {
                result.getCredentials();
            } catch (AmazonClientException e) {
                throw new RuntimeException("Missing AWS credentials and no instance role found", e);
            }
        } else {
            result = new AWSStaticCredentialsProvider(new BasicAWSCredentials(awsSecretKeyId, awsSecretAccessKey));
        }
        return result;
    }

    private ObjectMetadata copyFile(ObjectMetadata objectMetadata, String bucket, String sourceKey, String targetKey, boolean deleteSource) {
        CopyObjectRequest copyObjectRequest = new CopyObjectRequest(bucket, sourceKey, bucket, targetKey);
        CopyObjectResult copyObjectResult = s3Client.copyObject(copyObjectRequest);

        if (deleteSource) {
            s3Client.deleteObject(bucket, sourceKey);
        }

        return s3Client.getObjectMetadata(bucket, targetKey);
    }

    private ObjectMetadata copyBigFile(ObjectMetadata objectMetadata, String bucket, String sourceKey, String targetKey, boolean deleteSource) {
        List<CopyPartResult> copyResponses = new LinkedList<>();


        InitiateMultipartUploadRequest initiateMultipartUploadRequest = new InitiateMultipartUploadRequest(bucket, targetKey);

        InitiateMultipartUploadResult initiateMultipartUploadResult = s3Client.initiateMultipartUpload(initiateMultipartUploadRequest);

        String uploadId = initiateMultipartUploadResult.getUploadId();

        try {
            long objectSize = objectMetadata.getContentLength(); // in bytes

            // Step 4. Copy parts.
            long partSize = 20 * (long) Math.pow(2.0, 20.0); // 5 MB
            long bytePosition = 0;
            for (int i = 1; bytePosition < objectSize; ++i) {
                // Step 5. Save copy response.
                CopyPartRequest copyRequest = new CopyPartRequest()
                        .withDestinationBucketName(bucket)
                        .withDestinationKey(targetKey)
                        .withSourceBucketName(bucket)
                        .withSourceKey(sourceKey)
                        .withUploadId(uploadId)
                        .withFirstByte(bytePosition)
                        .withLastByte(bytePosition + partSize - 1 >= objectSize ? objectSize - 1 : bytePosition + partSize - 1)
                        .withPartNumber(i);

                copyResponses.add(s3Client.copyPart(copyRequest));
                bytePosition += partSize;
            }

            CompleteMultipartUploadRequest completeRequest = new CompleteMultipartUploadRequest(bucket, targetKey, uploadId, responsesToETags(copyResponses));

            CompleteMultipartUploadResult completeUploadResponse =
                    s3Client.completeMultipartUpload(completeRequest);


            if (deleteSource) {
                s3Client.deleteObject(bucket, sourceKey);
            }

            return s3Client.getObjectMetadata(bucket, targetKey);
        } catch (Exception e) {
            log.error(e);
            throw new RuntimeException(e);
        }
    }

    private List<PartETag> responsesToETags(List<CopyPartResult> responses) {
        return responses.stream().map(response -> new PartETag(response.getPartNumber(), response.getETag())).collect(Collectors.toList());
    }
}
