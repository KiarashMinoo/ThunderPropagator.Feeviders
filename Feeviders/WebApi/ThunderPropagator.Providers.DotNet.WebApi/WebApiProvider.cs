using System.Diagnostics;
using Microsoft.Extensions.Logging;
using ThunderPropagator.Providers.DotNet.SharedKernel;

namespace ThunderPropagator.Providers.DotNet.WebApi
{
    internal
#if !DEBUG
        sealed
#endif
        partial class WebApiProvider<TWebApiProviderMessage, TWebApiProviderConfiguration> : AbstractProvider<TWebApiProviderMessage, TWebApiProviderConfiguration>
        where TWebApiProviderMessage : WebApiProviderMessage
        where TWebApiProviderConfiguration : WebApiProviderConfiguration
    {
        private static partial class Log
        {
            [LoggerMessage(EventId = 4802, Level = LogLevel.Error, Message = "error has occured while posting message to path {Path}.")]
            public static partial void PostException(ILogger logger, Exception exception, string path);

            [LoggerMessage(EventId = 4803, Level = LogLevel.Warning, Message = "Exception while disposing HttpClient.")]
            public static partial void DisposeException(ILogger logger, Exception exception);
        }

        private readonly HttpClient _httpClient;
        private readonly TWebApiProviderConfiguration _webApiProviderConfiguration;

        public WebApiProvider(HttpClient httpClient,
            TWebApiProviderConfiguration webApiProviderConfiguration,
            IServiceProvider serviceProvider)
            : base(serviceProvider)
        {
            _httpClient = httpClient;
            _webApiProviderConfiguration = webApiProviderConfiguration;
        }

        protected override async Task InternalExecuteAsync(byte[] bytes, CancellationToken cancellationToken = default)
        {
            using var activity = WebApiProviderExtensions.ActivitySource.StartActivity("webapi publish", ActivityKind.Producer);
            activity?.SetTag("messaging.system", "webapi");
            activity?.SetTag("messaging.destination.name", _webApiProviderConfiguration.Path);
            activity?.SetTag("messaging.operation", "publish");

            var publishTimestamp = Stopwatch.GetTimestamp();
            try
            {
                using var request = new HttpRequestMessage(HttpMethod.Post, _webApiProviderConfiguration.Path);
                request.Content = new ByteArrayContent(bytes);
                var response = await _httpClient.SendAsync(request, cancellationToken).ConfigureAwait(false);
                response.EnsureSuccessStatusCode();

                WebApiProviderExtensions.MessagesPublished.Add(1);
            }
            catch (Exception exception)
            {
                activity?.SetStatus(ActivityStatusCode.Error, exception.Message);
                WebApiProviderExtensions.MessagesPublishFailed.Add(1);
                Log.PostException(Logger, exception, _webApiProviderConfiguration.Path);
                throw;
            }
            finally
            {
                WebApiProviderExtensions.PublishDuration.Record(Stopwatch.GetElapsedTime(publishTimestamp).TotalMilliseconds);
            }
        }

        protected override void DisposeManagedResources()
        {
            try
            {
                _httpClient?.Dispose();
            }
            catch (Exception ex)
            {
                Log.DisposeException(Logger, ex);
            }
        }
    }
}