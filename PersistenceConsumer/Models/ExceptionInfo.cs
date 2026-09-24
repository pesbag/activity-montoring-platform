using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Numerics;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace PersistenceConsumer.Models;

public class ExceptionInfo
{
    public BigInteger Id { get; set; }
    [Required]
    [MinLength(1)]
    [JsonPropertyName("event_id")]
    public string? EventId { get; set; }
    [Required]
    [MinLength(1)]
    [JsonPropertyName("source_id")]
    public string? SourceId { get; set; }
    [JsonPropertyName("detected_at")]
    [Required]
    public DateTime? DetectedAt { get; set; }
    [JsonPropertyName("value")]
    [Required]
    public double? Value { get; set; }
    [Required]
    [JsonPropertyName("mean")]
    public double Mean { get; set; }
    [Required]
    [JsonPropertyName("standard_deviation")]
    public double StandardDeviation { get; set; }
    [JsonPropertyName("z_score")]
    public double? ZScore { get; set; }
    [Required]
    [RegularExpression("^(New|Investigating|Resolved)$")]
    [JsonPropertyName("severity")]
    public string Severity { get; set; } = string.Empty;
    [StringLength(20)]
    [Required]
    [JsonPropertyName("status")]
    [MinLength(1)]
    public string Status { get; set; }
}
