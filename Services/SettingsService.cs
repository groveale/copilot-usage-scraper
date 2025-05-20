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
        string AzureFunctionsEnvironment { get; }
        string KeyVaultUrl { get; }
        string KeyVaultSecretName { get; }
        bool CDXTenant { get; }
        string RedirectUri { get; }
        string ServiceAccountUpn { get; }
        string KeyVaultEncryptionKeySecretName { get; }
        
    }

    public class SettingsService : ISettingsService
    {
        public string TenantId => Environment.GetEnvironmentVariable("AZURE_TENANT_ID");
        public string ClientId => Environment.GetEnvironmentVariable("AZURE_CLIENT_ID");
        public string ClientSecret => Environment.GetEnvironmentVariable("AZURE_CLIENT_SECRET");
        public string AzureFunctionsEnvironment => Environment.GetEnvironmentVariable("AZURE_FUNCTIONS_ENVIRONMENT");
        public string KeyVaultUrl => Environment.GetEnvironmentVariable("KeyVault:Url");
        public string KeyVaultSecretName => Environment.GetEnvironmentVariable("KeyVault:SecretName");
        public string StorageAccountUri => Environment.GetEnvironmentVariable("StorageAccountUri");
        public string StorageAccountName => Environment.GetEnvironmentVariable("StorageAccountName");
        public string StorageAccountKey => Environment.GetEnvironmentVariable("StorageAccountKey");
        public bool CDXTenant => Environment.GetEnvironmentVariable("CDXTenant") == "true";
        public string RedirectUri => Environment.GetEnvironmentVariable("RedirectUri");
        public string ServiceAccountUpn => Environment.GetEnvironmentVariable("SERVICE_ACCOUNT_UPN");
        public string KeyVaultEncryptionKeySecretName => Environment.GetEnvironmentVariable("KeyVault:EncryptionKeySecretName");

    }
}