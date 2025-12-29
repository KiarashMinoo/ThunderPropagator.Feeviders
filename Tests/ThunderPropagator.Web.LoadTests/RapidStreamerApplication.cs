using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace ThunderPropagator.Web.LoadTests;

internal
#if !DEBUG
    sealed
#endif
    class ThunderPropagatorApplication : WebApplicationFactory<Program>
{
    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        builder.ConfigureServices(services => services.Configure<HealthCheckServiceOptions>(options => options.Registrations.Clear()));
    }
}