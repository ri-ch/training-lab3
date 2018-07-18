using System;
using System.IO;
using System.Net;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Microsoft.Extensions.Configuration;

namespace DynamoLab
{
    class Program
    {
        private static IConfiguration _config;
        static void Main(string[] args)
        {
            _config = 
                new ConfigurationBuilder()
                .SetBasePath(AppContext.BaseDirectory)
                .AddJsonFile("appsettings.json")
                .Build();

            var options = _config.GetAWSOptions();
            IAmazonDynamoDB client = options.CreateServiceClient<IAmazonDynamoDB>();

            ListTablesResponse response = client.ListTablesAsync().Result;

            Console.WriteLine(response.HttpStatusCode == HttpStatusCode.OK ? "Success" : "Failure");
            Console.ReadLine();
        }
    }
}
