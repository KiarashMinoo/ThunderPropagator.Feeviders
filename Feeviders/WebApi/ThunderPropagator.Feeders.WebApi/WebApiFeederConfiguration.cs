using ThunderPropagator.Application.Feeders;
using ThunderPropagator.Feeders.Inbox;

namespace ThunderPropagator.Feeders.WebApi
{
    public abstract class WebApiFeederConfiguration : AbstractFeederConfiguration
    {
        public string Path
        {
            get => Get<string>()!;
            set => Set(value);
        }

        /// <summary>Opt-in transactional Inbox configuration for this Feevider. Disabled by default.</summary>
        public InboxOptions Inbox
        {
            get => Get(new InboxOptions());
            set => Set(value);
        }
    }
}