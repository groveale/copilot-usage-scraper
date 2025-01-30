using Azure.Identity;
using Azure.Security.KeyVault.Secrets;
using Microsoft.Extensions.Configuration;
using Microsoft.Identity.Client;
using Microsoft.Graph;
using System.Text.Json.Serialization;
using System.Text.Json;

namespace groveale
{
    public interface ITokenService
    {
        Task<string> GetAccessTokenAsync();
        Task<string> RefreshAccessTokenAsync();
    }

    public class TokenService : ITokenService
    {
        private readonly IConfiguration _configuration;
        private readonly SecretClient _secretClient;
        private readonly string _tokenEndpoint;
        private readonly string _clientId;
        private readonly string _clientSecret;
        private readonly string _scope;
        private string _cachedAccessToken;
        private DateTime _tokenExpiresAt;

        public TokenService(IConfiguration configuration)
        {
            _configuration = configuration;
            
            // Initialize Key Vault client
            var keyVaultUrl = _configuration["KeyVault:Url"];
            var credential = new DefaultAzureCredential();
            _secretClient = new SecretClient(new Uri(keyVaultUrl), credential);

            // Initialize token-related fields
            _tokenEndpoint = _configuration["AzureAd:TokenEndpoint"];
            _clientId = _configuration["AzureAd:ClientId"];
            _clientSecret = _configuration["AzureAd:ClientSecret"];
            _scope = "https://graph.microsoft.com/.default";
        }

        public async Task<string> GetAccessTokenAsync()
        {
            // Check if we have a valid cached token
            if (!string.IsNullOrEmpty(_cachedAccessToken) && DateTime.UtcNow < _tokenExpiresAt)
            {
                return _cachedAccessToken;
            }

            return await RefreshAccessTokenAsync();
        }

        public async Task<string> RefreshAccessTokenAsync()
        {
            try
            {
                // Get refresh token from Key Vault
                var refreshTokenSecret = await _secretClient.GetSecretAsync(_configuration["KeyVault:RefreshTokenSecretName"]);
                var refreshToken = refreshTokenSecret.Value.Value;

                using (var httpClient = new HttpClient())
                {
                    var request = new Dictionary<string, string>
                    {
                        { "client_id", _clientId },
                        { "client_secret", _clientSecret },
                        { "refresh_token", refreshToken },
                        { "grant_type", "refresh_token" },
                        { "scope", _scope }
                    };

                    var response = await httpClient.PostAsync(_tokenEndpoint, new FormUrlEncodedContent(request));
                    response.EnsureSuccessStatusCode();

                    var content = await response.Content.ReadAsStringAsync();
                    var tokenResponse = JsonSerializer.Deserialize<TokenResponse>(content);

                    _cachedAccessToken = tokenResponse.AccessToken;
                    _tokenExpiresAt = DateTime.UtcNow.AddSeconds(tokenResponse.ExpiresIn);

                    // Store the new refresh token if it was provided
                    if (!string.IsNullOrEmpty(tokenResponse.RefreshToken) && tokenResponse.RefreshToken != refreshToken)
                    {
                        await _secretClient.SetSecretAsync(
                            _configuration["KeyVault:RefreshTokenSecretName"],
                            tokenResponse.RefreshToken);
                    }

                    return tokenResponse.AccessToken;
                }
            }
            catch (Exception ex)
            {
                throw new Exception("Failed to refresh access token", ex);
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

