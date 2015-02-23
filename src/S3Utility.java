import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

public class S3Utility {


    static AmazonS3 dreamObjectsS3;
    static AmazonS3 amazonS3;
    static Properties properties = new Properties();



    private static void loadProperties() throws FileNotFoundException,IOException {

        InputStream is = new FileInputStream("s3migration.properties");
        properties.load(is);

    }
    private static String getValueFromProperties(String keyName){
       return properties.getProperty(keyName);
    }


    private static void init() throws Exception {

        loadProperties();
        dreamObjectsS3 = new AmazonS3Client(new BasicAWSCredentials(getValueFromProperties("doKey"),getValueFromProperties("doSecret") ));
        dreamObjectsS3.setEndpoint("objects.dreamhost.com");

        amazonS3 = new AmazonS3Client(new BasicAWSCredentials(getValueFromProperties("awsKey"), getValueFromProperties("awsSecret")));

    }

    public static void listObjectsFromS3(AmazonS3 s3, String sourceBucket, String destBucket, Long dateAfter) {
        try {
            List<Bucket> buckets = s3.listBuckets();

            long totalSize = 0;
            int totalItems = 0;
            // for (Bucket bucket : buckets) {
                /*
                 * In order to save bandwidth, an S3 object listing does not
                 * contain every object in the bucket; after a certain point the
                 * S3ObjectListing is truncated, and further pages must be
                 * obtained with the AmazonS3Client.listNextBatchOfObjects()
                 * method.
                 */
            Bucket bucket = new Bucket();
            bucket.setName(sourceBucket);

            System.out.println(bucket.getName());
            ObjectListing objects = s3.listObjects(bucket.getName());

            do {
                for (S3ObjectSummary objectSummary : objects.getObjectSummaries()) {
                    totalSize += objectSummary.getSize();
                    System.out.println(objectSummary.getKey() + ": " + objectSummary.getLastModified());

                    try {

                        if (!objectSummary.getKey().endsWith(".mov") && dateAfter != null && objectSummary.getLastModified().getTime() >= dateAfter)
                            uploadObject(amazonS3, downloadObject(dreamObjectsS3, sourceBucket, objectSummary.getKey()), destBucket);
                        else if (!objectSummary.getKey().endsWith(".mov")&&dateAfter == null)
                            uploadObject(amazonS3, downloadObject(dreamObjectsS3, sourceBucket, objectSummary.getKey()), destBucket);

                    } catch (Exception e) {
                        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                    }


                    totalItems++;
                }
                objects = s3.listNextBatchOfObjects(objects);

            } while (objects.isTruncated());
            //  }

            System.out.println(buckets.toArray().toString());

            System.out.println("You have " + buckets.size() + " Amazon S3 bucket(s), " +
                    "containing " + totalItems + " objects with a total size of " + totalSize + " bytes.");
        } catch (AmazonServiceException ase) {
            /*
             * AmazonServiceExceptions represent an error response from an AWS
             * services, i.e. your request made it to AWS, but the AWS service
             * either found it invalid or encountered an error trying to execute
             * it.
             */

        } catch (AmazonClientException ace) {
            ace.printStackTrace();

            System.out.println("Error Message: " + ace.getMessage());
        }
    }

    public static void uploadObject(AmazonS3 s3, S3Object object, String destBucket) {
        PutObjectRequest objectRequest = new PutObjectRequest(destBucket,
                object.getKey(),
                object.getObjectContent(),
                object.getObjectMetadata());
        System.out.println("copying " + object.getKey());
        s3.putObject(objectRequest);
    }

    public static S3Object downloadObject(AmazonS3 s3, String bucket, String key) {

        return s3.getObject(bucket, key);

//   it was failing to getObject() from dreamobjects with the new version of sdk so had to do this ugliness

//        GeneratePresignedUrlRequest req= new GeneratePresignedUrlRequest(bucket,key);
//        System.out.println(s3.generatePresignedUrl(req));
//        HttpClient client =  HttpClientBuilder.create().build();
//        HttpGet get = new HttpGet(s3.generatePresignedUrl(req).toString());
//        File file =  new File("/archive/"+key);
//        try {
//            HttpResponse response = client.execute(get);
//
//            file.getParentFile().mkdirs();
//            byte[] fileBytes = IOUtils.toByteArray(response.getEntity().getContent());
//            if(fileBytes==null||fileBytes.length==0)
//                return null;
//
//            FileCopyUtils.copy(fileBytes,file);
//
//
//        } catch (IOException e) {
//          e.printStackTrace();
//        }
//        return file;
    }

    public static void main(String[] args) throws Exception {
        System.out.println(args.length);

        if (args.length < 2) {
            System.out.println("appname <sourcebucket> <destbucket> <longDateAfter optional>");
            System.out.println("please supply source and dest buckets");
            System.exit(1);
        }
        Long dateAfter = null;
        String sourceBucket = args[0];
        String destBucket = args[1];

        if (args.length==3&&args[2] != null && !args[2].isEmpty())
        {
            try {
                dateAfter = Long.parseLong(args[2]);
            } catch (NumberFormatException nfe) {
                nfe.printStackTrace();
                System.exit(1);
            }
        }
        init();
        System.out.println("=================dreamObjectsS3==========================");
        // downloadObject(dreamObjectsS3,"stagebucket","2014-11-25-15/3edc12ac-a24c-450e-b5b1-4665404db716-thumb.jpg");
        listObjectsFromS3(dreamObjectsS3, sourceBucket, destBucket, dateAfter);
        System.out.println("=================amazonS3==========================");
        //listObjectsFromS3(amazonS3);

    }
}
