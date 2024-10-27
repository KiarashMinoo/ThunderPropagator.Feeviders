using Microsoft.Extensions.Logging;
using RapidStreamer.Providers.DotNet.SharedKernel;

namespace RapidStreamer.Providers.DotNet.WebApi
{
    internal
#if !DEBUG
        sealed
#endif
        class WebApiProvider<TWebApiProviderMessage, TWebApiProviderConfiguration> : AbstractProvider<TWebApiProviderMessage, TWebApiProviderConfiguration>
        where TWebApiProviderMessage : WebApiProviderMessage
        where TWebApiProviderConfiguration : WebApiProviderConfiguration
    {
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
                var response = await _httpClient.SendAsync(request, cancellationToken);
                response.EnsureSuccessStatusCode();
            }
            catch (Exception exception)
            {
                Logger.LogError(exception,
                    "error has occured while posting message to path {Path}.",
                    _webApiProviderConfiguration.Path);
                throw;
            }
        }

        protected override void DisposeManagedResources()
        {
            _httpClient.Dispose();
        }
    }
}