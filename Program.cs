using System;
using System.IO;
using System.Net;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Microsoft.Extensions.Configuration;

namespace DynamoLab
{
    class Program
    {
        private static IConfiguration _config;
        static async Task Main(string[] args)
        {
            _config = 
                new ConfigurationBuilder()
                .SetBasePath(AppContext.BaseDirectory)
                .AddJsonFile("appsettings.json")
                .Build();

            ListTablesResponse response = null;

            using (var client = _config.GetAWSOptions().CreateServiceClient<IAmazonDynamoDB>())
            {
                response = await client.ListTablesAsync();
            }

            var output = response != null && response.HttpStatusCode == HttpStatusCode.OK ? "Success" : "Failure";
            Console.WriteLine(output);
            Console.ReadLine();
        }
    }
}
