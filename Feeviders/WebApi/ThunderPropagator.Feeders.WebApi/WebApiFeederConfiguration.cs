using ThunderPropagator.Application.Feeders;

namespace ThunderPropagator.Feeders.WebApi
{
    public abstract class WebApiFeederConfiguration : AbstractFeederConfiguration
    {
        public string Path
        {
            get => Get<string>()!;
            set => Set(value);
        }
    }
}