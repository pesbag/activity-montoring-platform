using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MongoDB.Driver;
using RawConsumer.Services;
using System.IO;

var builder = Host.CreateDefaultBuilder(args);

builder.ConfigureServices((hostContext, services) =>
{
    var configuration = hostContext.Configuration;

    var consumerConfig = new ConsumerConfig();
    configuration.GetSection("Kafka:Consumer").Bind(consumerConfig);
    services.AddSingleton(consumerConfig);

    services.AddSingleton<IMongoClient>(sp =>
        new MongoClient(configuration["MongoDb:ConnectionString"]));

    services.AddSingleton<IMongoDatabase>(sp =>
    {
        var client = sp.GetRequiredService<IMongoClient>();
        return client.GetDatabase(configuration["MongoDb:DatabaseName"]);
    });

    services.AddHostedService<MongoConsumerService>();
});

await builder.Build().RunAsync();