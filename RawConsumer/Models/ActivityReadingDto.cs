using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace RawConsumer.Model;

public class ActivityReadingDto
{
    [Required]
    [MinLength(1)]
    [JsonPropertyName("event_id")]
    public string? EventId { get; set; }
    [Required]
    [MinLength(1)]
    [JsonPropertyName("source_id")]
    public string? SourceId { get; set; }
    [JsonPropertyName("timestamp")]
    [Required]
    public DateTime? TimeStamp { get; set; }
    [JsonPropertyName("value")]
    [Required]
    public double? Value { get; set; }
}
