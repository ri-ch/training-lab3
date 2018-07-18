using System;
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

            Console.WriteLine("Hello World!");
        }
    }
}
