using Confluent.Kafka;
using Consumer.Services;
using Consumer.Data;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

var configuration = new ConfigurationBuilder()
    .SetBasePath(Directory.GetCurrentDirectory())
    .AddJsonFile("appsettings.json", optional: false)
    .Build();

string connectionString = configuration["ConnectionStrings:PackageDb"]!;
var services = new ServiceCollection();
services.AddDbContext<DeliverDbContext>(opt => opt.UseMySql(connectionString, ServerVersion.AutoDetect(connectionString)));
services.AddScoped<ConsumerServices>();
var serviceProvider = services.BuildServiceProvider();

using (var scope = serviceProvider.CreateScope())
{
    scope.ServiceProvider.GetRequiredService<DeliverDbContext>().Database.EnsureCreated();
}
var consumerConfig = new ConsumerConfig
{
    BootstrapServers = configuration["Kafka:BootstrapServices"],
    GroupId = configuration["Kafka:GroupId"],
    AutoOffsetReset = AutoOffsetReset.Earliest,
    EnableAutoCommit = false,
    AllowAutoCreateTopics = true
};

using var consumer = new ConsumerBuilder<Ignore, string>(consumerConfig).Build();
try
{
    await ConsumeTopicAsync(configuration["Kafka:Topics:Courier"]!);
    await ConsumeTopicAsync(configuration["Kafka:Topics:Deliver"]!);
}
catch (OperationCanceledException)
{
    Console.WriteLine("\nshutting down");
}
finally
{
    consumer.Close();
    Console.WriteLine("consumer closed");
}
async Task ConsumeTopicAsync(string topic)
{
    Console.WriteLine("{DateTime.Now} received from topic: {result.Topic}");

    using var scope = serviceProvider.CreateScope();
    var service = scope.ServiceProvider.GetRequiredService<ConsumerServices>();

    bool isSuccess = false;
    if (topic == configuration["Kafka:Topics:Courier"])
    {
        isSuccess = await service.ProcessCourierModelAsync(result.Message.Value);
    }
    else if (topic == configuration["Kafka:Topics:Deliver"])
    {
        isSuccess = await service.ProcessDeliverModelASync(result.Message.Value);
    }
    if (isSuccess)
    {
        consumer.Commit(result);
    }
}