// TokenService.cs
using Azure.Identity;
using Azure.Security.KeyVault.Secrets;
using groveale.Services;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace groveale
{
    public interface ITokenService
    {
        Task<string> GetAccessTokenAsync();
        Task<string> GetRefreshTokenFromCodeAsync(string authcode);
    }

    public class TokenService : ITokenService
    {
        private readonly ISettingsService _settings;
        private readonly SecretClient _secretClient;
        private readonly string _tokenEndpoint;
        private readonly string _clientId;
        private readonly string _clientSecret;
        private readonly string _scope;
        private string _cachedAccessToken;
        private DateTime _tokenExpiresAt;
        private readonly string _environment;

        public TokenService(ISettingsService settings)
        {
            _settings = settings;
            _environment = settings.AzureFunctionsEnvironment;
            
            var keyVaultUrl = _settings.KeyVaultUrl;

            if (!IsDevelopment())
            {
                // Initialize Key Vault client
                
                var credential = new DefaultAzureCredential();
                _secretClient = new SecretClient(new Uri(keyVaultUrl), credential);
            }
            else
            {
                // Initialize Key Vault client
                var credential = new ClientSecretCredential(
                    System.Environment.GetEnvironmentVariable("AZURE_TENANT_ID"), 
                    System.Environment.GetEnvironmentVariable("AZURE_CLIENT_ID"),
                    System.Environment.GetEnvironmentVariable("AZURE_CLIENT_SECRET"));
                _secretClient = new SecretClient(new Uri(keyVaultUrl), credential);
            }

            // Initialize token-related fields
            _tokenEndpoint = $"https://login.microsoftonline.com/{_settings.TenantId}/oauth2/v2.0/token";
            _clientId = _settings.ClientId;
            _clientSecret = _settings.ClientSecret;
            _scope = "https://graph.microsoft.com/.default";
        }

        private bool IsDevelopment()
        {
            // todo: remove this
            return true;
            //return string.Equals(_environment, "Development", StringComparison.OrdinalIgnoreCase);
        }

        public async Task<string> GetAccessTokenAsync()
        {
            if (!string.IsNullOrEmpty(_cachedAccessToken) && DateTime.UtcNow < _tokenExpiresAt)
            {
                return _cachedAccessToken;
            }

            string refreshToken = await GetRefreshTokenAsync();
            return await RefreshAccessTokenAsync(refreshToken);
        }

        private async Task<string> GetRefreshTokenAsync()
        {
            var secret = await _secretClient.GetSecretAsync(_settings.KeyVaultSecretName);
            return secret.Value.Value;
        }

        public async Task<string> GetRefreshTokenFromCodeAsync(string authcode)
        {
            return await RefreshAccessTokenAsync(authcode, true);
        }

        private async Task<string> RefreshAccessTokenAsync(string refreshToken, bool isAuthCode = false)
        {
            using var httpClient = new HttpClient();
            var request = new Dictionary<string, string>
            {
                { "client_id", _clientId },
                { "client_secret", _clientSecret },
                { "refresh_token", refreshToken },
                { "grant_type", "refresh_token" },
                { "scope", _scope }
            };

            if (isAuthCode)
            {
                request["grant_type"] = "authorization_code";
                request.Remove("refresh_token");
                request.Add("code", refreshToken);
                request.Add("redirect_uri", _settings.RedirectUri);
            }

            var response = await httpClient.PostAsync(_tokenEndpoint, new FormUrlEncodedContent(request));
            response.EnsureSuccessStatusCode();

            var content = await response.Content.ReadAsStringAsync();
            var tokenResponse = JsonSerializer.Deserialize<TokenResponse>(content);

            _cachedAccessToken = tokenResponse.AccessToken;
            _tokenExpiresAt = DateTime.UtcNow.AddSeconds(tokenResponse.ExpiresIn);

            if (!string.IsNullOrEmpty(tokenResponse.RefreshToken) && tokenResponse.RefreshToken != refreshToken)
            {
                await StoreNewRefreshTokenAsync(tokenResponse.RefreshToken);
            }

            return tokenResponse.AccessToken;
        }

        private async Task StoreNewRefreshTokenAsync(string newRefreshToken)
        {
            try {
                await _secretClient.SetSecretAsync(
                _settings.KeyVaultSecretName,
                newRefreshToken);
            }
            catch (Exception ex)
            {
                throw new Exception("Failed to store new refresh token", ex);
            }
            
        }

        private class TokenResponse
        {
            [JsonPropertyName("access_token")]
            public string AccessToken { get; set; }

            [JsonPropertyName("refresh_token")]
            public string RefreshToken { get; set; }

            [JsonPropertyName("expires_in")]
            public int ExpiresIn { get; set; }
        }
    }
}