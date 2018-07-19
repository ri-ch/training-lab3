using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.S3;
using Amazon.S3.Model;
using Microsoft.Extensions.Configuration;

namespace DynamoLab
{
    class Program
    {
        private static IConfiguration _config;
        private const string _tableName = "Infections";
        
        private const string _indexName = "InfectionsByCityDate";
        static async Task Main(string[] args)
        {
            _config =
                new ConfigurationBuilder()
                .SetBasePath(AppContext.BaseDirectory)
                .AddJsonFile("appsettings.json")
                .Build();

            using (var client = _config.GetAWSOptions().CreateServiceClient<IAmazonDynamoDB>())
            {
                // await CreateTableAndIndex(client);
                // await UploadDataToTable(client);
                await GetRenoInfections(client);
                // Add report url for each patient
            }

            Console.WriteLine("Done");
            Console.ReadLine();
        }

        private static async Task GetRenoInfections(IAmazonDynamoDB ddbClient)
        {
            QueryRequest qr = new QueryRequest();
            qr.TableName = _tableName;
            qr.IndexName = _indexName;
            qr.KeyConditionExpression = "City = :v_City";
            qr.ExpressionAttributeValues = new Dictionary<string, AttributeValue>() {
                { ":v_City", new AttributeValue("Reno") }
            };

            QueryResponse queryResponse = await ddbClient.QueryAsync(qr);

            Console.WriteLine($"Found {queryResponse.Items.Count} items");
        }

        private static async Task UploadDataToTable(IAmazonDynamoDB ddbClient)
        {
            using (var s3Client = _config.GetAWSOptions().CreateServiceClient<IAmazonS3>())
            {
                GetObjectRequest getObjectRequest = new GetObjectRequest();
                getObjectRequest.BucketName = _config["S3BucketName"];
                getObjectRequest.Key = _config["InfectionsDataFile"];
                GetObjectResponse response = await s3Client.GetObjectAsync(getObjectRequest);
                using (TextReader reader = new StreamReader(response.ResponseStream))
                {
                    string line = string.Empty;
                    while ((line = reader.ReadLine()) != null)
                    {
                        var parts = line.Split(",");

                        if (parts[0].ToLower().Equals("patientid") == false)
                        {
                            var putItemRequest = new PutItemRequest();
                            putItemRequest.TableName = _tableName;
                            putItemRequest.Item = 
                                new Dictionary<string, AttributeValue>() {
                                    { "PatientID", new AttributeValue(parts[0]) },
                                    { "City", new AttributeValue(parts[1]) },
                                    { "Date", new AttributeValue(parts[2]) },
                                };

                            await ddbClient.PutItemAsync(putItemRequest);

                            Console.WriteLine($"Added item: {parts[0]} - {parts[1]} - {parts[2]}");
                        }
                    }
                }
            }
        }

        private static async Task CreateTableAndIndex(IAmazonDynamoDB client)
        {
            ListTablesResponse listTablesResponse = await client.ListTablesAsync();

            if (listTablesResponse.TableNames.Contains(_tableName))
            {
                Console.WriteLine("Table already exists!. Deleting Table...");
                await client.DeleteTableAsync(_tableName);
                await Task.Delay(10000);
            }

            CreateTableRequest request = new CreateTableRequest();

            var attributeDefinitions = new[] {
                new AttributeDefinition {
                    AttributeName = "PatientID",
                    AttributeType = ScalarAttributeType.S,
                },
                new AttributeDefinition {
                    AttributeName = "City",
                    AttributeType = ScalarAttributeType.S,
                },
                new AttributeDefinition {
                    AttributeName = "Date",
                    AttributeType = ScalarAttributeType.S,
                }
            }.ToList();

            var gsi = new GlobalSecondaryIndex();
            gsi.IndexName = _indexName;
            gsi.Projection = new Projection { ProjectionType = "ALL" };
            gsi.ProvisionedThroughput = 
                new ProvisionedThroughput
                {
                    ReadCapacityUnits = 5L,
                    WriteCapacityUnits = 5L
                };

            gsi.KeySchema = new[] {
                new KeySchemaElement("City", KeyType.HASH),
                new KeySchemaElement("Date", KeyType.RANGE)
            }.ToList();

            request.TableName = _tableName;
            request.AttributeDefinitions = attributeDefinitions;
            request.KeySchema = new[] {
                new KeySchemaElement("PatientID", KeyType.HASH)
            }.ToList();

            request.GlobalSecondaryIndexes = new[] {
                gsi
            }.ToList();

            request.ProvisionedThroughput =
                new ProvisionedThroughput
                {
                    ReadCapacityUnits = 5L,
                    WriteCapacityUnits = 5L
                };

            CreateTableResponse response = await client.CreateTableAsync(request);

            if (response.HttpStatusCode != HttpStatusCode.OK)
                Console.WriteLine("Table creation failed!");

            bool tableCreated = false;

            Console.Write("Creating table....");

            while (tableCreated == false)
            {
                await Task.Delay(500);
                DescribeTableResponse describeResponse = await client.DescribeTableAsync(_tableName);
                tableCreated = describeResponse.Table.TableStatus == TableStatus.ACTIVE;
                Console.Write("..");
            }

            Console.WriteLine();
        }
    }
}
