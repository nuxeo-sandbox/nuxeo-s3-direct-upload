package org.nuxeo.s3.blob;

import java.io.IOException;

import org.nuxeo.ecm.core.api.Blob;
import org.nuxeo.ecm.core.blob.BlobManager;
import org.nuxeo.ecm.core.blob.binary.BinaryBlobProvider;
import org.nuxeo.ecm.core.storage.sql.CloudFrontBinaryManager;

import com.amazonaws.services.s3.model.ObjectMetadata;

public class CustomCloudFrontBinaryManager extends CloudFrontBinaryManager {

    @Override
    public Blob readBlob(BlobManager.BlobInfo blobInfo) throws IOException {
        if (blobInfo.length == null) {
            String digest = blobInfo.key;
            // strip prefix
            int colon = digest.indexOf(':');
            if (colon >= 0) {
                digest = digest.substring(colon + 1);
            }
            ObjectMetadata metadata = amazonS3.getObjectMetadata(bucketName, digest);
            blobInfo.length = metadata.getContentLength();
            blobInfo.filename = metadata.getUserMetadata().get("filename");
            blobInfo.mimeType = metadata.getUserMetadata().get("mimeType");
            blobInfo.digest = digest;
        }

        // just delegate to avoid copy/pasting code
        return new BinaryBlobProvider(this).readBlob(blobInfo);
    }

    @Override
    public boolean performsExternalAccessControl(BlobManager.BlobInfo blobInfo) {
        return true;
    }
}
