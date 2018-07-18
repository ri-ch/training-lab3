using System;
using Amazon.DynamoDBv2;
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

            Console.WriteLine("Hello World!");
        }
    }
}
