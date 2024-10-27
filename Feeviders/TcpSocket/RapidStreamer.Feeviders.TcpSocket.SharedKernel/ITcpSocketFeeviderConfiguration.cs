namespace RapidStreamer.Feeviders.TcpSocket.SharedKernel
{
    public interface ITcpSocketFeeviderConfiguration
    {
        bool? Ssl { get; set; }
        short Port { get; set; }
        int BufferSize { get; set; }
        string? Username { get; set; }
        string? Password { get; set; }
        int? ReadTimeout { get; set; }
        int? WriteTimeout { get; set; }
    }
}