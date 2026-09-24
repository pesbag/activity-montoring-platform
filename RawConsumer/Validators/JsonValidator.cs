using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace RawConsumer.Validators;

public static class JsonValidator
{
    private static readonly JsonSerializerOptions SerializerOptions = new()
    {
        PropertyNameCaseInsensitive = true,
        NumberHandling = JsonNumberHandling.AllowReadingFromString
    };
    public static bool TryValidateJson<T>(string jsonString, out T result, out List<ValidationResult> validationResults)
        where T : class
    {
        validationResults = new List<ValidationResult>();
        result = null;

        try
        {
            result = JsonSerializer.Deserialize<T>(jsonString, SerializerOptions);

            if (result == null)
            {
                validationResults.Add(new ValidationResult("payload deserialized to null"));
                return false;
            }

            var context = new ValidationContext(result, serviceProvider: null, items: null);
            return Validator.TryValidateObject(result, context, validationResults, validateAllProperties: true);
        }
        catch (JsonException ex)
        {
            validationResults.Add(new ValidationResult($"invalid json syntax: {ex.Message}"));
            return false;
        }
    }
}