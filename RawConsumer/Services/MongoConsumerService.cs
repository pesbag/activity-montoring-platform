using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
//using MongoDB.Bson;
using MongoDB.Driver;
using RawConsumer.Model;
using RawConsumer.Validators;



namespace RawConsumer.Services;

public class MongoConsumerService: BackgroundService
{
    private readonly ConsumerConfig _consumerConfig;
    //private readonly IMongoCollection<BsonDocument> _collection;
    private readonly IMongoCollection<ActivityReading> _collection;
    private readonly string _topic;
    private readonly ILogger<MongoConsumerService> _logger;
    public MongoConsumerService(
       ConsumerConfig consumerConfig,
       IMongoDatabase database,
       IConfiguration configuration,
       ILogger<MongoConsumerService> logger)
    {
        _consumerConfig = consumerConfig;
        //_collection = database.GetCollection<BsonDocument>("clean-reading");
        _collection = database.GetCollection<ActivityReading>("clean-reading");
        _topic = configuration["Kafka:Topic"]!;
        _logger = logger;
    }
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Starting MongoConsumerService for topic: {Topic}", _topic);
        await Task.Run(async () =>
        {
            using var consumer = new ConsumerBuilder<Ignore, string>(_consumerConfig).Build();
            consumer.Subscribe(_topic);

            try
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    ConsumeResult<Ignore, string>? consumeResult = null;

                    try
                    {
                        consumeResult = consumer.Consume(stoppingToken);
                    }
                    catch (OperationCanceledException) { break; }
                    catch (ConsumeException ex)
                    {
                        _logger.LogError(ex, "Kafka consumption error: {Reason}", ex.Error.Reason);
                        Console.WriteLine($"error: {ex.Error.Reason}");
                        continue;
                    }

                    if (consumeResult?.Message?.Value == null || string.IsNullOrWhiteSpace(consumeResult.Message.Value)) { continue; }
                    string rawJson = consumeResult.Message.Value;

                    try
                    {
                        //var document = BsonDocument.Parse(consumeResult.Message.Value);
                        bool isValid = JsonValidator.TryValidateJson<ActivityReading>(rawJson, out var eventDto, out var errors);
                        if (isValid)
                        {
                            await _collection.InsertOneAsync(eventDto, cancellationToken: stoppingToken);
                            _logger.LogDebug("Saved event {EventId} to Mongo", eventDto.EventId);
                            //await _collection.InsertOneAsync(document, cancellationToken: stoppingToken);
                        }
                        else
                        {
                            _logger.LogWarning("Invalid message format: {Errors}", string.Join(", ", errors));
                        }
                        consumer.Commit(consumeResult);

                        
                    }
                    catch (FormatException jsonEx)
                    {
                        Console.WriteLine($"error in json: {jsonEx.Message}");
                        consumer.Commit(consumeResult);
                    }
                    catch (MongoException mongoEx)
                    {
                        Console.WriteLine($"error in saving to MongoDB: {mongoEx.Message}");
                    }
                }
            }
            finally
            {
                _logger.LogInformation("enter to finally scope");
                consumer.Close();
            }
        }, stoppingToken);
    }
}
