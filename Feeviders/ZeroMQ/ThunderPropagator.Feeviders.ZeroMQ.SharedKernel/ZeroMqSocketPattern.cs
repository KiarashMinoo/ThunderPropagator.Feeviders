namespace ThunderPropagator.Feeviders.ZeroMQ.SharedKernel
{
    /// <summary>
    /// The ZeroMQ socket pattern pair used by a Feeder/Provider.
    /// </summary>
    public enum ZeroMqSocketPattern
    {
        /// <summary>Fan-out, best-effort delivery: Provider uses PUB, Feeder uses SUB.</summary>
        PubSub,

        /// <summary>Load-balanced, at-least-once within a session: Provider uses PUSH, Feeder uses PULL.</summary>
        PushPull
    }
}
