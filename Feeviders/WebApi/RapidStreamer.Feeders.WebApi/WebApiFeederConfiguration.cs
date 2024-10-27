using RapidStreamer.Application.Feeders;

namespace RapidStreamer.Feeders.WebApi
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