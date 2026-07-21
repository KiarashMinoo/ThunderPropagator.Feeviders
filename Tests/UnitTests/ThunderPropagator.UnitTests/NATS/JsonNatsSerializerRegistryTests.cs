using ThunderPropagator.BuildingBlocks.Application.Serializations;
using ThunderPropagator.BuildingBlocks.Application.Serializations.Json;
using ThunderPropagator.Feeders.SharedKernel;
using ThunderPropagator.Feeviders.NATS.SharedKernel;
using ThunderPropagator.Providers.DotNet.SharedKernel;
using Xunit.Abstractions;

namespace ThunderPropagator.UnitTests.NATS;

public class JsonNatsSerializerRegistryTests
{
    private readonly ITestOutputHelper _output;

    public JsonNatsSerializerRegistryTests(ITestOutputHelper output)
    {
        _output = output;
    }

    public static IEnumerable<object[]> SerializerTypes()
    {
        yield return [JsonFormatSerializer.Json];
        yield return [NJsonFormatSerializer.NJson];
    }

    private static FormatSerializerInvoker CreateFormatSerializerInvoker()
    {
        IFormatSerializer jsonFormatSerializer = new JsonFormatSerializer();
        IFormatSerializer nJsonFormatSerializer = new NJsonFormatSerializer();

        return serializerType => serializerType.Value == JsonFormatSerializer.Json.Value ? jsonFormatSerializer : nJsonFormatSerializer;
    }

    private static FormatDeserializerInvoker CreateFormatDeserializerInvoker()
    {
        IFormatDeserializer jsonFormatDeserializer = new JsonFormatSerializer();
        IFormatDeserializer nJsonFormatDeserializer = new NJsonFormatSerializer();

        return serializerType => serializerType.Value == JsonFormatSerializer.Json.Value ? jsonFormatDeserializer : nJsonFormatDeserializer;
    }

    private static JsonNatsSerializerRegistry CreateRegistry(SerializerType serializerType)
        => new(CreateFormatDeserializerInvoker(), CreateFormatSerializerInvoker(), serializerType);

    [Theory]
    [MemberData(nameof(SerializerTypes))]
    public void Constructor_WithValidSerializerType_ShouldNotThrow(SerializerType serializerType)
    {
        // Act
        var exception = Record.Exception(() => CreateRegistry(serializerType));

        // Assert
        Assert.Null(exception);
    }

    [Theory]
    [MemberData(nameof(SerializerTypes))]
    public void GetSerializer_ShouldReturnSerializer(SerializerType serializerType)
    {
        // Arrange
        var registry = CreateRegistry(serializerType);

        // Act
        var serializer = registry.GetSerializer<TestMessage>();

        // Assert
        Assert.NotNull(serializer);
        Assert.IsAssignableFrom<JsonNatsSerializer<TestMessage>>(serializer);
    }

    [Theory]
    [MemberData(nameof(SerializerTypes))]
    public void GetDeserializer_ShouldReturnDeserializer(SerializerType serializerType)
    {
        // Arrange
        var registry = CreateRegistry(serializerType);

        // Act
        var deserializer = registry.GetDeserializer<TestMessage>();

        // Assert
        Assert.NotNull(deserializer);
        Assert.IsAssignableFrom<JsonNatsDeserializer<TestMessage>>(deserializer);
    }

    [Theory]
    [MemberData(nameof(SerializerTypes))]
    public void GetSerializer_CalledTwice_ShouldReturnSameInstance(SerializerType serializerType)
    {
        // Arrange
        var registry = CreateRegistry(serializerType);

        // Act
        var serializer1 = registry.GetSerializer<TestMessage>();
        var serializer2 = registry.GetSerializer<TestMessage>();

        // Assert
        Assert.Same(serializer1, serializer2);
    }

    [Theory]
    [MemberData(nameof(SerializerTypes))]
    public void GetDeserializer_CalledTwice_ShouldReturnSameInstance(SerializerType serializerType)
    {
        // Arrange
        var registry = CreateRegistry(serializerType);

        // Act
        var deserializer1 = registry.GetDeserializer<TestMessage>();
        var deserializer2 = registry.GetDeserializer<TestMessage>();

        // Assert
        Assert.Same(deserializer1, deserializer2);
    }

    [Theory]
    [MemberData(nameof(SerializerTypes))]
    public void GetSerializer_WithDifferentTypes_ShouldReturnDifferentInstances(SerializerType serializerType)
    {
        // Arrange
        var registry = CreateRegistry(serializerType);

        // Act
        var serializer1 = registry.GetSerializer<TestMessage>();
        var serializer2 = registry.GetSerializer<AnotherTestMessage>();

        // Assert
        Assert.NotSame(serializer1, serializer2);
    }

    [Theory]
    [MemberData(nameof(SerializerTypes))]
    public void GetDeserializer_WithDifferentTypes_ShouldReturnDifferentInstances(SerializerType serializerType)
    {
        // Arrange
        var registry = CreateRegistry(serializerType);

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
