
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.ComponentModel.DataAnnotations;
using System.Text.Json.Serialization;
using System.Diagnostics.CodeAnalysis;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Bson.Serialization.Attributes;
namespace RawConsumer.Model;

public class ActivityReading
{
    [BsonId]
    //[BsonRepresentation(BsonType.ObjectId)]
    public ObjectId _id { get; set; }
    [Required]
    [NotNull]
    [JsonPropertyName("event_id")]
    public string EventId { get; set; } = string.Empty;
    [Required]
    [NotNull]
    [JsonPropertyName("source_id")]
    public string SourceId { get; set; } = string.Empty;
    [JsonPropertyName("timestamp")]
    [NotNull]
    public DateTime TimeStamp { get; set; }
    [JsonPropertyName("value")]
    [NotNull]
    public double Value { get; set; }
}
