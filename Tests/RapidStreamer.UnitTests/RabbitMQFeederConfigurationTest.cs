using Newtonsoft.Json.Schema;
using NJsonSchema.Generation;
using RapidStreamer.Feeders.RabbitMQ;
using Xunit.Abstractions;
using JsonSchema = NJsonSchema.JsonSchema;

namespace RapidStreamer.UnitTests
{
    public class RabbitMQFeederConfigurationTest
    {
        private readonly ITestOutputHelper _testOutputHelper;

        public RabbitMQFeederConfigurationTest(ITestOutputHelper testOutputHelper)
        {
            _testOutputHelper = testOutputHelper;
        }

        [Fact]
        public void JsonSchema_Should_Generate_Correctly()
        {
            //Arrange && Act
            bool gotException = false;
            Exception? exception = null;
            JsonSchema? schema = null;

            try
            {
                schema = NJsonSchema.JsonSchema.FromType<TestRabbitMQFeederConfiguration>(new SystemTextJsonSchemaGeneratorSettings()
                {
                    DefaultReferenceTypeNullHandling = ReferenceTypeNullHandling.Null,
                });
            }
            catch (Exception e)
            {
                _testOutputHelper.WriteLine(e.ToString());
                gotException = true;
                exception = e;
            }

            //Assert
            Assert.False(gotException);
            Assert.Null(exception);
            Assert.NotNull(schema);
        }

        private class TestRabbitMQFeederConfiguration : RabbitMQFeederConfiguration;
    }
}