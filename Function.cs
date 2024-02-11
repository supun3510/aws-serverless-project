using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.Lambda.Core;
using Amazon.Lambda.S3Events;


using Amazon.Rekognition;
using Amazon.Rekognition.Model;

using Amazon.S3;
using Amazon.S3.Model;
using Newtonsoft.Json;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace Lambda_Lab
{
    public class Function
    {
      
        private readonly IAmazonS3 _s3Client = new AmazonS3Client();
        private readonly IAmazonDynamoDB _dynamoDbClient = new AmazonDynamoDBClient();
        private const string BucketName = "test-message-bucket-01";
        private const string TableName = "message-store";
        public async Task FunctionHandler(Message message, ILambdaContext context)
        {

            // Validate the message
            if (!ValidateMessage(message))
            {
                return;
            }

            // Save to S3
            await SaveToS3(message);

            // Save to DynamoDB
            await SaveToDynamoDB(message);

        }

        private bool ValidateMessage(Message message)
        {
            // Return true if valid, false otherwise
            return message.metadata != null &&
                   !string.IsNullOrEmpty(message.metadata.message_time) &&
                   !string.IsNullOrEmpty(message.metadata.company_id) &&
                   !string.IsNullOrEmpty(message.metadata.message_id);
        }

        private async Task SaveToS3(Message message)
        {
            var putObjectRequest = new PutObjectRequest
            {
                BucketName = BucketName,
                Key = $"{message.metadata.company_id}/{message.metadata.message_id}.json",
                ContentBody = JsonConvert.SerializeObject(message),
                ContentType = "application/json"
            };

            await _s3Client.PutObjectAsync(putObjectRequest);
        }

        private async Task SaveToDynamoDB(Message message)
        {
            var table = Table.LoadTable(_dynamoDbClient, TableName);

            var document = new Document();
            document["message_id"] = message.metadata.message_id;
            document["order_id"] = message.data.order_id;
            document["order_time"] = message.data.order_time;
            document["order_amount"] = message.data.order_amount;

            await table.PutItemAsync(document);
        }
    }

    public class Message
    {
        public Metadata metadata { get; set; }
        public Data data { get; set; }
    }

    public class Metadata
    {
        public string message_time { get; set; }
        public string company_id { get; set; }
        public string message_id { get; set; }
    }

    public class Data
    {
        public string order_id { get; set; }
        public string order_time { get; set; }
        public string order_amount { get; set; }
    }

}
