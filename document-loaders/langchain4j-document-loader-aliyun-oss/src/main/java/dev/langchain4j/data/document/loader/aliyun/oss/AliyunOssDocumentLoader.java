package dev.langchain4j.data.document.loader.aliyun.oss;

import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.common.auth.CredentialsProvider;
import com.aliyun.oss.common.auth.DefaultCredentialProvider;
import com.aliyun.oss.common.auth.DefaultCredentials;
import com.aliyun.oss.common.auth.ProfileCredentialsProvider;
import com.aliyun.oss.model.*;
import dev.langchain4j.data.document.Document;
import dev.langchain4j.data.document.DocumentLoader;
import dev.langchain4j.data.document.DocumentParser;
import dev.langchain4j.data.document.source.aliyun.oss.AliyunOssSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static dev.langchain4j.internal.Utils.isNullOrBlank;
import static dev.langchain4j.internal.ValidationUtils.ensureNotBlank;
import static java.util.stream.Collectors.toList;

public class AliyunOssDocumentLoader {

    private static final Logger log = LoggerFactory.getLogger(AliyunOssDocumentLoader.class);

    private final OSS ossClient;

    public AliyunOssDocumentLoader(OSS ossClient) {
        this.ossClient = ossClient;
    }

    /**
     * Loads a single document from the specified OSS bucket based on the specified object key.
     *
     * @param bucket OSS bucket to load from.
     * @param key    The key of the OSS object which should be loaded.
     * @param parser The parser to be used for parsing text from the object.
     * @return A document containing the content of the OSS object.
     * @throws RuntimeException If {@link OSSException} or {@link ClientException} occurs.
     */
    public Document loadDocument(String bucket, String key, DocumentParser parser) {
        try {
            GetObjectRequest getObjectRequest = new GetObjectRequest(ensureNotBlank(bucket, "bucket"), ensureNotBlank(key, "key"));
            OSSObject object = ossClient.getObject(getObjectRequest);
            AliyunOssSource source = new AliyunOssSource(object.getObjectContent(), bucket, key);
            return DocumentLoader.load(source, parser);
        } catch (OSSException | ClientException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Loads all documents from an OSS bucket.
     * Skips any documents that fail to load.
     *
     * @param bucket OSS bucket to load from.
     * @param parser The parser to be used for parsing text from the object.
     * @return A list of documents.
     * @throws RuntimeException If {@link OSSException} or {@link ClientException} occurs.
     */
    public List<Document> loadDocuments(String bucket, DocumentParser parser) {
        return loadDocuments(bucket, null, parser);
    }

    /**
     * Loads all documents from an OSS bucket.
     * Skips any documents that fail to load.
     *
     * @param bucket OSS bucket to load from.
     * @param prefix Only keys with the specified prefix will be loaded.
     * @param parser The parser to be used for parsing text from the object.
     * @return A list of documents.
     * @throws RuntimeException If {@link OSSException} or {@link ClientException} occurs.
     */
    public List<Document> loadDocuments(String bucket, String prefix, DocumentParser parser) {
        List<Document> documents = new ArrayList<>();

        ListObjectsV2Request listObjectsV2Request = new ListObjectsV2Request(ensureNotBlank(bucket, "bucket"), prefix);

        ListObjectsV2Result listObjectsV2Result = ossClient.listObjectsV2(listObjectsV2Request);

        List<OSSObjectSummary> filteredOssObjects = listObjectsV2Result.getObjectSummaries().stream()
                .filter(ossObject -> !ossObject.getKey().endsWith("/") && ossObject.getSize() > 0)
                .collect(toList());

        for (OSSObjectSummary ossObjectSummary : filteredOssObjects) {
            String key = ossObjectSummary.getKey();
            try {
                Document document = loadDocument(bucket, key, parser);
                documents.add(document);
            } catch (Exception e) {
                log.warn("Failed to load an object with key '{}' from bucket '{}', skipping it.", key, bucket, e);
            }
        }

        return documents;
    }

    public void shutdown() {
        ossClient.shutdown();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private String region;
        private String endpointUrl;
        private String profile;
        private DefaultCredentials aliyunCredentials;

        /**
         * Set the Aliyun region.
         *
         * @param region The Aliyun region.
         * @return The builder instance.
         */
        public Builder region(String region) {
            this.region = region;
            return this;
        }

        /**
         * Specifies a custom endpoint URL to override the default service URL.
         *
         * @param endpointUrl The endpoint URL.
         * @return The builder instance.
         */
        public Builder endpointUrl(String endpointUrl) {
            this.endpointUrl = endpointUrl;
            return this;
        }

        /**
         * Set the profile defined in Aliyun credentials. If not set, it will use the default profile.
         *
         * @param profile The profile defined in Aliyun credentials.
         * @return The builder instance.
         */
        public Builder profile(String profile) {
            this.profile = profile;
            return this;
        }

        /**
         * Set the Aliyun credentials. If not set, it will use the default credentials.
         *
         * @param aliyunCredentials The Aliyun credentials.
         * @return The builder instance.
         */
        public Builder aliyunCredentials(DefaultCredentials aliyunCredentials) {
            this.aliyunCredentials = aliyunCredentials;
            return this;
        }

        public AliyunOssDocumentLoader build() {
            CredentialsProvider credentialsProvider = createCredentialsProvider();
            OSS ossClient = createOssClient(credentialsProvider);
            return new AliyunOssDocumentLoader(ossClient);
        }

        private CredentialsProvider createCredentialsProvider() {
            if (!isNullOrBlank(profile)) {
                return new ProfileCredentialsProvider(profile);
            }

            return new DefaultCredentialProvider(aliyunCredentials);
        }

        private OSS createOssClient(CredentialsProvider credentialsProvider) {
            OSSClientBuilder.OSSClientBuilderImpl ossClientBuilder = OSSClientBuilder.create()
                    .region(region)
                    .credentialsProvider(credentialsProvider);

            if (!isNullOrBlank(endpointUrl)) {
                ossClientBuilder.endpoint(endpointUrl);
            }

            return ossClientBuilder.build();
        }
    }
}
