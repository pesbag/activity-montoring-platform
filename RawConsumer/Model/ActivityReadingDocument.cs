using System;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace RawConsumer.Model;

public class ActivityReadingDocument
{
    [BsonId]
    public ObjectId Id { get; set; }

    [BsonElement("event_id")]
    public string EventId { get; set; } = null!;

    [BsonElement("source_id")]
    public string SourceId { get; set; } = null!;

    [BsonElement("timestamp")]
    public DateTime TimeStamp { get; set; }

    [BsonElement("value")]
    public double Value { get; set; }
}