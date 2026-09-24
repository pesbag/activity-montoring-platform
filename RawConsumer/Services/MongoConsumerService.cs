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
    private readonly IMongoCollection<ActivityReadingDocument> _collection;
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
        _collection = database.GetCollection<ActivityReadingDocument>("clean-reading");
        _topic = configuration["Kafka:Topic"]!;
        _logger = logger;
    }
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("starting MongoConsumerService for topic: {Topic}", _topic);
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
                        _logger.LogError(ex, "kafka consumption error: {Reason}", ex.Error.Reason);
                        Console.WriteLine($"error: {ex.Error.Reason}");
                        continue;
                    }

                    if (consumeResult?.Message?.Value == null || string.IsNullOrWhiteSpace(consumeResult.Message.Value)) { continue; }
                    string rawJson = consumeResult.Message.Value;

                    try
                    {
                        bool isValid = JsonValidator.TryValidateJson<ActivityReadingDto>(rawJson, out var eventDto, out var errors);

                        if (isValid && eventDto is not null)
                        {
                            var document = new ActivityReadingDocument
                            {
                                EventId = eventDto.EventId!,
                                SourceId = eventDto.SourceId!,
                                TimeStamp = eventDto.TimeStamp!.Value,
                                Value = eventDto.Value!.Value
                            };

                            await _collection.InsertOneAsync(document, cancellationToken: stoppingToken);
                            _logger.LogDebug("saved event {eventId} to mongo", document.EventId);
                        }
                        else
                        {
                            _logger.LogWarning("invalid message format: {errors}", string.Join(", ", errors));
                        }
                        consumer.Commit(consumeResult);
                    }
                    catch (FormatException jsonEx)
                    {
                        _logger.LogError(jsonEx, "error in json format: {message}", jsonEx.Message);
                        consumer.Commit(consumeResult);
                    }
                    catch (MongoException mongoEx)
                    {
                        _logger.LogError(mongoEx, "error in saving to MongoDB: {message}", mongoEx.Message);
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
