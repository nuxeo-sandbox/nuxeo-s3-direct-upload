package org.nuxeo.s3.upload;

import static org.junit.Assert.assertNotNull;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.nuxeo.ecm.platform.test.PlatformFeature;
import org.nuxeo.runtime.test.runner.Deploy;
import org.nuxeo.runtime.test.runner.Features;
import org.nuxeo.runtime.test.runner.FeaturesRunner;

import com.google.inject.Inject;

@RunWith(FeaturesRunner.class)
@Features({ PlatformFeature.class })
@Deploy("org.nuxeo.s3.upload.nuxeo-s3-direct-upload-core")
public class TestS3Upload {

    @Inject
    protected S3Upload s3upload;

    @Test
    public void testService() {
        assertNotNull(s3upload);
    }
}
