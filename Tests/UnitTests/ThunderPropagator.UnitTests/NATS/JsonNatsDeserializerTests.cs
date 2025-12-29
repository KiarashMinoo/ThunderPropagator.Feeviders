using System.Buffers;
using ThunderPropagator.BuildingBlocks.Application.Serializations;
using ThunderPropagator.Feeviders.NATS.SharedKernel;
using Xunit.Abstractions;

namespace ThunderPropagator.UnitTests.NATS;

public class JsonNatsDeserializerTests
{
    private readonly ITestOutputHelper _output;

    public JsonNatsDeserializerTests(ITestOutputHelper output)
    {
        _output = output;
    }

    [Theory]
    [InlineData(SerializerType.Json)]
    [InlineData(SerializerType.NJson)]
    [InlineData(SerializerType.NetJson)]
    public void Constructor_WithValidSerializerType_ShouldNotThrow(SerializerType serializerType)
    {
        // Act
        var exception = Record.Exception(() => new JsonNatsDeserializer<TestMessage>(serializerType));

        // Assert
        Assert.Null(exception);
    }

    [Fact]
    public void Deserialize_WithValidBuffer_ShouldReturnMessage()
    {
        // Arrange
        var serializer = new JsonNatsSerializer<TestMessage>(SerializerType.Json);
        var deserializer = new JsonNatsDeserializer<TestMessage>(SerializerType.Json);
        var originalMessage = new TestMessage { Id = "123", Content = "Test content" };
        var bufferWriter = new ArrayBufferWriter<byte>();
        serializer.Serialize(bufferWriter, originalMessage);
        var buffer = new ReadOnlySequence<byte>(bufferWriter.WrittenMemory);

        // Act
        var result = deserializer.Deserialize(buffer);

        // Assert
        Assert.NotNull(result);
        Assert.Equal(originalMessage.Id, result.Id);
        Assert.Equal(originalMessage.Content, result.Content);
    }

    [Theory]
    [InlineData(SerializerType.Json)]
    [InlineData(SerializerType.NJson)]
    [InlineData(SerializerType.NetJson)]
    public void Deserialize_WithEmptyMessage_ShouldReturnEmptyMessage(SerializerType serializerType)
    {
        // Arrange
        var serializer = new JsonNatsSerializer<TestMessage>(serializerType);
        var deserializer = new JsonNatsDeserializer<TestMessage>(serializerType);
        var originalMessage = new TestMessage();
        var bufferWriter = new ArrayBufferWriter<byte>();
        serializer.Serialize(bufferWriter, originalMessage);
        var buffer = new ReadOnlySequence<byte>(bufferWriter.WrittenMemory);

        // Act
        var result = deserializer.Deserialize(buffer);

        // Assert
        Assert.NotNull(result);
    }

    [Fact]
    public void Deserialize_WithComplexMessage_ShouldReturnCompleteMessage()
    {
        // Arrange
        var serializer = new JsonNatsSerializer<ComplexMessage>(SerializerType.Json);
        var deserializer = new JsonNatsDeserializer<ComplexMessage>(SerializerType.Json);
        var originalMessage = new ComplexMessage
        {
            Id = 42,
            Name = "Test",
            Timestamp = DateTime.Parse("2025-12-03T10:00:00Z").ToUniversalTime(),
            Values = new[] { 1, 2, 3 },
            Metadata = new Dictionary<string, string> { ["key"] = "value" }
        };
        var bufferWriter = new ArrayBufferWriter<byte>();
        serializer.Serialize(bufferWriter, originalMessage);
        var buffer = new ReadOnlySequence<byte>(bufferWriter.WrittenMemory);

        // Act
        var result = deserializer.Deserialize(buffer);

        // Assert
        Assert.NotNull(result);
        Assert.Equal(originalMessage.Id, result.Id);
        Assert.Equal(originalMessage.Name, result.Name);
        Assert.NotNull(result.Values);
        Assert.Equal(originalMessage.Values.Length, result.Values.Length);
    }

    [Fact]
    public void Deserialize_MultipleMessages_ShouldDeserializeEachCorrectly()
    {
        // Arrange
        var serializer = new JsonNatsSerializer<TestMessage>(SerializerType.Json);
        var deserializer = new JsonNatsDeserializer<TestMessage>(SerializerType.Json);
        var messages = new[]
        {
            new TestMessage { Id = "1", Content = "First" },
            new TestMessage { Id = "2", Content = "Second" },
            new TestMessage { Id = "3", Content = "Third" }
        };

        // Act & Assert
        foreach (var originalMessage in messages)
        {
            var bufferWriter = new ArrayBufferWriter<byte>();
            serializer.Serialize(bufferWriter, originalMessage);
            var buffer = new ReadOnlySequence<byte>(bufferWriter.WrittenMemory);
            var result = deserializer.Deserialize(buffer);
            
            Assert.NotNull(result);
            Assert.Equal(originalMessage.Id, result.Id);
            Assert.Equal(originalMessage.Content, result.Content);
        }
    }

    [Fact]
    public void Deserialize_RoundTrip_ShouldPreserveData()
    {
        // Arrange
        var serializer = new JsonNatsSerializer<TestMessage>(SerializerType.Json);
        var deserializer = new JsonNatsDeserializer<TestMessage>(SerializerType.Json);
        var originalMessage = new TestMessage { Id = "round-trip-test", Content = "Testing round trip serialization" };
        
        // Act - Serialize
        var bufferWriter = new ArrayBufferWriter<byte>();
        serializer.Serialize(bufferWriter, originalMessage);
        var buffer = new ReadOnlySequence<byte>(bufferWriter.WrittenMemory);
        
        // Act - Deserialize
        var deserializedMessage = deserializer.Deserialize(buffer);
        
        // Act - Serialize again
        var bufferWriter2 = new ArrayBufferWriter<byte>();
        serializer.Serialize(bufferWriter2, deserializedMessage!);
        var buffer2 = new ReadOnlySequence<byte>(bufferWriter2.WrittenMemory);
        
        // Act - Deserialize again
        var result = deserializer.Deserialize(buffer2);

        // Assert
        Assert.NotNull(result);
        Assert.Equal(originalMessage.Id, result.Id);
        Assert.Equal(originalMessage.Content, result.Content);
    }

    private class TestMessage
    {
        public string? Id { get; set; }
        public string? Content { get; set; }
    }

    private class ComplexMessage
    {
        public int Id { get; set; }
        public string? Name { get; set; }
        public DateTime Timestamp { get; set; }
        public int[]? Values { get; set; }
        public Dictionary<string, string>? Metadata { get; set; }
    }
}
