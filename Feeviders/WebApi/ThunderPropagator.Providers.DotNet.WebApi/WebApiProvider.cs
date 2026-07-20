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
            try
            {
                using var request = new HttpRequestMessage(HttpMethod.Post, _webApiProviderConfiguration.Path);
                request.Content = new ByteArrayContent(bytes);
                var response = await _httpClient.SendAsync(request, cancellationToken).ConfigureAwait(false);
                response.EnsureSuccessStatusCode();
            }
            catch (Exception exception)
            {
                Log.PostException(Logger, exception, _webApiProviderConfiguration.Path);
                throw;
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