namespace ThunderPropagator.Feeviders.AwsSqs.SharedKernel;

public interface IAwsFeeviderConfiguration
{
    string? RegionSystemName { get; set; }
    string? ServiceUrl { get; set; }
    string? AccessKey { get; set; }
    string? SecretKey { get; set; }
}
