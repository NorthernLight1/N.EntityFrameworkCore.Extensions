using System;
using System.Collections.Generic;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace N.EntityFrameworkCore.Extensions.Util;

internal static class QueryToJsonFileUtil
{
    private static readonly JsonWriterOptions IndentedWriterOptions = new() { Indented = true };
    private static readonly JsonWriterOptions CompactWriterOptions = new() { Indented = false };

    internal static QueryToFileResult WriteEntities<T>(IEnumerable<T> entities, Stream stream, QueryToJsonFileOptions options) where T : class
    {
        var properties = GetEntityProperties(typeof(T));
        using var writer = CreateJsonWriter(stream, options);
        int dataRowCount = WriteEntityArray(writer, entities, properties);
        writer.Flush();
        return CreateResult(stream, dataRowCount);
    }

    internal static QueryToFileResult WriteEntitiesAsLines<T>(IEnumerable<T> entities, Stream stream, QueryToJsonFileOptions options) where T : class
    {
        var properties = GetEntityProperties(typeof(T));
        int dataRowCount = 0;
        foreach (var entity in entities)
        {
            WriteEntityLine(stream, properties, entity, options);
            dataRowCount++;
        }
        return CreateResult(stream, dataRowCount);
    }

    internal static async Task<QueryToFileResult> WriteEntitiesAsLinesAsync<T>(IAsyncEnumerable<T> entities, Stream stream, QueryToJsonFileOptions options,
        CancellationToken cancellationToken) where T : class
    {
        var properties = GetEntityProperties(typeof(T));
        int dataRowCount = 0;
        await foreach (var entity in entities.WithCancellation(cancellationToken))
        {
            WriteEntityLine(stream, properties, entity, options);
            dataRowCount++;
        }
        return CreateResult(stream, dataRowCount);
    }

    internal static async Task<QueryToFileResult> WriteEntitiesAsync<T>(IAsyncEnumerable<T> entities, Stream stream, QueryToJsonFileOptions options,
        CancellationToken cancellationToken) where T : class
    {
        var properties = GetEntityProperties(typeof(T));
        await using var writer = CreateJsonWriter(stream, options);
        writer.WriteStartArray();
        int dataRowCount = 0;
        await foreach (var entity in entities.WithCancellation(cancellationToken))
        {
            WriteEntityObject(writer, properties, entity);
            dataRowCount++;
        }
        writer.WriteEndArray();
        await writer.FlushAsync(cancellationToken);
        return CreateResult(stream, dataRowCount);
    }

    internal static QueryToFileResult WriteDataReader(DbDataReader reader, Stream stream, QueryToJsonFileOptions options)
    {
        using var writer = CreateJsonWriter(stream, options);
        int dataRowCount = WriteReaderArray(writer, reader, readAsync: false, cancellationToken: default).GetAwaiter().GetResult();
        writer.Flush();
        return CreateResult(stream, dataRowCount);
    }

    internal static QueryToFileResult WriteDataReaderAsLines(DbDataReader reader, Stream stream, QueryToJsonFileOptions options)
    {
        int dataRowCount = WriteReaderLines(reader, stream, options, readAsync: false, cancellationToken: default).GetAwaiter().GetResult();
        return CreateResult(stream, dataRowCount);
    }

    internal static async Task<QueryToFileResult> WriteDataReaderAsync(DbDataReader reader, Stream stream, QueryToJsonFileOptions options,
        CancellationToken cancellationToken)
    {
        await using var writer = CreateJsonWriter(stream, options);
        int dataRowCount = await WriteReaderArray(writer, reader, readAsync: true, cancellationToken);
        await writer.FlushAsync(cancellationToken);
        return CreateResult(stream, dataRowCount);
    }

    internal static async Task<QueryToFileResult> WriteDataReaderAsLinesAsync(DbDataReader reader, Stream stream, QueryToJsonFileOptions options,
        CancellationToken cancellationToken)
    {
        int dataRowCount = await WriteReaderLines(reader, stream, options, readAsync: true, cancellationToken);
        return CreateResult(stream, dataRowCount);
    }

    private static Utf8JsonWriter CreateJsonWriter(Stream stream, QueryToJsonFileOptions options) =>
        new(stream, options.Indented ? IndentedWriterOptions : CompactWriterOptions);

    private static PropertyInfo[] GetEntityProperties(Type entityType) =>
        entityType.GetProperties()
            .Where(p => p.CanRead && (!typeof(System.Collections.IEnumerable).IsAssignableFrom(p.PropertyType) || p.PropertyType == typeof(string)))
            .ToArray();

    private static int WriteEntityArray<T>(Utf8JsonWriter writer, IEnumerable<T> entities, PropertyInfo[] properties) where T : class
    {
        writer.WriteStartArray();
        int dataRowCount = 0;
        foreach (var entity in entities)
        {
            WriteEntityObject(writer, properties, entity);
            dataRowCount++;
        }
        writer.WriteEndArray();
        return dataRowCount;
    }

    private static void WriteEntityObject(Utf8JsonWriter writer, PropertyInfo[] properties, object entity)
    {
        writer.WriteStartObject();
        foreach (var property in properties)
            WriteNamedValue(writer, property.Name, property.GetValue(entity));
        writer.WriteEndObject();
    }

    private static void WriteEntityLine(Stream stream, PropertyInfo[] properties, object entity, QueryToJsonFileOptions options)
    {
        using var lineStream = new MemoryStream();
        using (var writer = new Utf8JsonWriter(lineStream, CompactWriterOptions))
        {
            WriteEntityObject(writer, properties, entity);
            writer.Flush();
        }
        lineStream.Position = 0;
        lineStream.CopyTo(stream);
        stream.WriteByte((byte)'\n');
    }

    private static void WriteReaderLine(Stream stream, DbDataReader reader, string[] columnNames, QueryToJsonFileOptions options)
    {
        using var lineStream = new MemoryStream();
        using (var writer = new Utf8JsonWriter(lineStream, CompactWriterOptions))
        {
            writer.WriteStartObject();
            for (int i = 0; i < columnNames.Length; i++)
                WriteNamedValue(writer, columnNames[i], reader.IsDBNull(i) ? null : reader.GetValue(i));
            writer.WriteEndObject();
            writer.Flush();
        }
        lineStream.Position = 0;
        lineStream.CopyTo(stream);
        stream.WriteByte((byte)'\n');
    }

    private static async Task<int> WriteReaderLines(DbDataReader reader, Stream stream, QueryToJsonFileOptions options, bool readAsync, CancellationToken cancellationToken)
    {
        int dataRowCount = 0;
        var fieldCount = reader.FieldCount;
        var columnNames = new string[fieldCount];
        for (int i = 0; i < fieldCount; i++)
            columnNames[i] = reader.GetName(i);

        while (readAsync ? await reader.ReadAsync(cancellationToken) : reader.Read())
        {
            WriteReaderLine(stream, reader, columnNames, options);
            dataRowCount++;
        }
        return dataRowCount;
    }

    private static async Task<int> WriteReaderArray(Utf8JsonWriter writer, DbDataReader reader, bool readAsync, CancellationToken cancellationToken)
    {
        writer.WriteStartArray();
        int dataRowCount = 0;
        var fieldCount = reader.FieldCount;
        var columnNames = new string[fieldCount];
        for (int i = 0; i < fieldCount; i++)
            columnNames[i] = reader.GetName(i);

        while (readAsync ? await reader.ReadAsync(cancellationToken) : reader.Read())
        {
            writer.WriteStartObject();
            for (int i = 0; i < fieldCount; i++)
                WriteNamedValue(writer, columnNames[i], reader.IsDBNull(i) ? null : reader.GetValue(i));
            writer.WriteEndObject();
            dataRowCount++;
        }
        writer.WriteEndArray();
        return dataRowCount;
    }

    private static void WriteNamedValue(Utf8JsonWriter writer, string name, object value)
    {
        writer.WritePropertyName(name);
        WriteJsonValue(writer, value);
    }

    private static void WriteJsonValue(Utf8JsonWriter writer, object value)
    {
        switch (value)
        {
            case null:
                writer.WriteNullValue();
                break;
            case string s:
                writer.WriteStringValue(s);
                break;
            case bool b:
                writer.WriteBooleanValue(b);
                break;
            case byte or sbyte or short or ushort or int or uint or long or ulong:
                writer.WriteNumberValue(Convert.ToInt64(value));
                break;
            case float f:
                writer.WriteNumberValue(f);
                break;
            case double d:
                writer.WriteNumberValue(d);
                break;
            case decimal dec:
                writer.WriteNumberValue(dec);
                break;
            case DateTime dt:
                writer.WriteStringValue(dt);
                break;
            case DateTimeOffset dto:
                writer.WriteStringValue(dto);
                break;
            case Guid g:
                writer.WriteStringValue(g);
                break;
            case Enum e:
                writer.WriteStringValue(e.ToString());
                break;
            default:
                JsonSerializer.Serialize(writer, value, value.GetType());
                break;
        }
    }

    private static QueryToFileResult CreateResult(Stream stream, int dataRowCount) =>
        new()
        {
            BytesWritten = stream.CanSeek ? stream.Length : 0,
            DataRowCount = dataRowCount,
            TotalRowCount = dataRowCount
        };
}
