using System.Buffers;
using ThunderPropagator.BuildingBlocks.Application.Serializations;
using ThunderPropagator.Feeviders.NATS.SharedKernel;
using Xunit.Abstractions;

namespace ThunderPropagator.UnitTests.NATS;

public class JsonNatsSerializerTests
{
    private readonly ITestOutputHelper _output;

    public JsonNatsSerializerTests(ITestOutputHelper output)
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
        var exception = Record.Exception(() => new JsonNatsSerializer<TestMessage>(serializerType));

        // Assert
        Assert.Null(exception);
    }

    [Theory]
    [InlineData(SerializerType.Json)]
    [InlineData(SerializerType.NJson)]
    [InlineData(SerializerType.NetJson)]
    public void Serialize_WithValidMessage_ShouldWriteToBuffer(SerializerType serializerType)
    {
        // Arrange
        var serializer = new JsonNatsSerializer<TestMessage>(serializerType);
        var message = new TestMessage { Id = "123", Content = "Test content" };
        var bufferWriter = new ArrayBufferWriter<byte>();

        // Act
        serializer.Serialize(bufferWriter, message);

        // Assert
        Assert.True(bufferWriter.WrittenCount > 0);
    }

    [Theory]
    [InlineData(SerializerType.Json)]
    [InlineData(SerializerType.NJson)]
    [InlineData(SerializerType.NetJson)]
    public void Serialize_WithEmptyMessage_ShouldWriteToBuffer(SerializerType serializerType)
    {
        // Arrange
        var serializer = new JsonNatsSerializer<TestMessage>(serializerType);
        var message = new TestMessage();
        var bufferWriter = new ArrayBufferWriter<byte>();

        // Act
        serializer.Serialize(bufferWriter, message);

        // Assert
        Assert.True(bufferWriter.WrittenCount > 0);
    }

    [Theory]
    [InlineData(SerializerType.Json)]
    [InlineData(SerializerType.NJson)]
    [InlineData(SerializerType.NetJson)]
    public void Serialize_WithComplexMessage_ShouldWriteToBuffer(SerializerType serializerType)
    {
        // Arrange
        var serializer = new JsonNatsSerializer<ComplexMessage>(serializerType);
        var message = new ComplexMessage
        {
            Id = 42,
            Name = "Test",
            Timestamp = DateTime.UtcNow,
            Values = new[] { 1, 2, 3 },
            Metadata = new Dictionary<string, string> { ["key"] = "value" }
        };
        var bufferWriter = new ArrayBufferWriter<byte>();

        // Act
        serializer.Serialize(bufferWriter, message);

        // Assert
        Assert.True(bufferWriter.WrittenCount > 0);
    }

    [Theory]
    [InlineData(SerializerType.Json)]
    [InlineData(SerializerType.NJson)]
    [InlineData(SerializerType.NetJson)]
    public void Serialize_MultipleMessages_ShouldWriteEachToBuffer(SerializerType serializerType)
    {
        // Arrange
        var serializer = new JsonNatsSerializer<TestMessage>(serializerType);
        var messages = new[]
        {
            new TestMessage { Id = "1", Content = "First" },
            new TestMessage { Id = "2", Content = "Second" },
            new TestMessage { Id = "3", Content = "Third" }
        };

        // Act & Assert
        foreach (var message in messages)
        {
            var bufferWriter = new ArrayBufferWriter<byte>();
            serializer.Serialize(bufferWriter, message);
            Assert.True(bufferWriter.WrittenCount > 0);
        }
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
