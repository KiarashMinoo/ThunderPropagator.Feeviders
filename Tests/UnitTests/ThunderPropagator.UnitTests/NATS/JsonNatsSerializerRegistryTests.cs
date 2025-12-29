using ThunderPropagator.BuildingBlocks.Application.Serializations;
using ThunderPropagator.Feeviders.NATS.SharedKernel;
using Xunit.Abstractions;

namespace ThunderPropagator.UnitTests.NATS;

public class JsonNatsSerializerRegistryTests
{
    private readonly ITestOutputHelper _output;

    public JsonNatsSerializerRegistryTests(ITestOutputHelper output)
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
        var exception = Record.Exception(() => new JsonNatsSerializerRegistry(serializerType));

        // Assert
        Assert.Null(exception);
    }

    [Theory]
    [InlineData(SerializerType.Json)]
    [InlineData(SerializerType.NJson)]
    [InlineData(SerializerType.NetJson)]
    public void GetSerializer_ShouldReturnSerializer(SerializerType serializerType)
    {
        // Arrange
        var registry = new JsonNatsSerializerRegistry(serializerType);

        // Act
        var serializer = registry.GetSerializer<TestMessage>();

        // Assert
        Assert.NotNull(serializer);
        Assert.IsAssignableFrom<JsonNatsSerializer<TestMessage>>(serializer);
    }

    [Theory]
    [InlineData(SerializerType.Json)]
    [InlineData(SerializerType.NJson)]
    [InlineData(SerializerType.NetJson)]
    public void GetDeserializer_ShouldReturnDeserializer(SerializerType serializerType)
    {
        // Arrange
        var registry = new JsonNatsSerializerRegistry(serializerType);

        // Act
        var deserializer = registry.GetDeserializer<TestMessage>();

        // Assert
        Assert.NotNull(deserializer);
        Assert.IsAssignableFrom<JsonNatsDeserializer<TestMessage>>(deserializer);
    }

    [Theory]
    [InlineData(SerializerType.Json)]
    [InlineData(SerializerType.NJson)]
    [InlineData(SerializerType.NetJson)]
    public void GetSerializer_CalledTwice_ShouldReturnSameInstance(SerializerType serializerType)
    {
        // Arrange
        var registry = new JsonNatsSerializerRegistry(serializerType);

        // Act
        var serializer1 = registry.GetSerializer<TestMessage>();
        var serializer2 = registry.GetSerializer<TestMessage>();

        // Assert
        Assert.Same(serializer1, serializer2);
    }

    [Theory]
    [InlineData(SerializerType.Json)]
    [InlineData(SerializerType.NJson)]
    [InlineData(SerializerType.NetJson)]
    public void GetDeserializer_CalledTwice_ShouldReturnSameInstance(SerializerType serializerType)
    {
        // Arrange
        var registry = new JsonNatsSerializerRegistry(serializerType);

        // Act
        var deserializer1 = registry.GetDeserializer<TestMessage>();
        var deserializer2 = registry.GetDeserializer<TestMessage>();

        // Assert
        Assert.Same(deserializer1, deserializer2);
    }

    [Theory]
    [InlineData(SerializerType.Json)]
    [InlineData(SerializerType.NJson)]
    [InlineData(SerializerType.NetJson)]
    public void GetSerializer_WithDifferentTypes_ShouldReturnDifferentInstances(SerializerType serializerType)
    {
        // Arrange
        var registry = new JsonNatsSerializerRegistry(serializerType);

        // Act
        var serializer1 = registry.GetSerializer<TestMessage>();
        var serializer2 = registry.GetSerializer<AnotherTestMessage>();

        // Assert
        Assert.NotSame(serializer1, serializer2);
    }

    [Theory]
    [InlineData(SerializerType.Json)]
    [InlineData(SerializerType.NJson)]
    [InlineData(SerializerType.NetJson)]
    public void GetDeserializer_WithDifferentTypes_ShouldReturnDifferentInstances(SerializerType serializerType)
    {
        // Arrange
        var registry = new JsonNatsSerializerRegistry(serializerType);

        // Act
        var deserializer1 = registry.GetDeserializer<TestMessage>();
        var deserializer2 = registry.GetDeserializer<AnotherTestMessage>();

        // Assert
        Assert.NotSame(deserializer1, deserializer2);
    }

    private class TestMessage
    {
        public string? Id { get; set; }
        public string? Content { get; set; }
    }

    private class AnotherTestMessage
    {
        public int Value { get; set; }
    }
}
