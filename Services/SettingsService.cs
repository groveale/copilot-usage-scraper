namespace groveale.Services
{
    public interface ISettingsService
    {
        string TenantId { get; }
        string ClientId { get; }
        string ClientSecret { get; }
        string StorageAccountUri { get; }
        string StorageAccountName { get; }
        string StorageAccountKey { get; }
        bool CDXTenant { get; }
        
    }

    public class SettingsService : ISettingsService
    {
        public string TenantId => Environment.GetEnvironmentVariable("AZURE_TENANT_ID");
        public string ClientId => Environment.GetEnvironmentVariable("AZURE_CLIENT_ID");
        public string ClientSecret => Environment.GetEnvironmentVariable("AZURE_CLIENT_SECRET");

        public string StorageAccountUri => Environment.GetEnvironmentVariable("StorageAccountUri");
        public string StorageAccountName => Environment.GetEnvironmentVariable("StorageAccountName");
        public string StorageAccountKey => Environment.GetEnvironmentVariable("StorageAccountKey");
        public bool CDXTenant => Environment.GetEnvironmentVariable("CDXTenant") == "true";

    }
}