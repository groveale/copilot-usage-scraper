//using Microsoft.Graph;
using Azure.Identity;
using Microsoft.Extensions.Logging;
using Microsoft.Graph.Beta;
using Microsoft.Graph.Beta.Models;
using System.Net.Http.Headers;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Kiota.Abstractions.Authentication;
using Azure.Security.KeyVault.Secrets;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Configuration;


namespace groveale.Services
{
    public interface IGraphDelegatedService
    {
        Task<ChatMessage> SendChatMessageToUserAsync(string message, string chatId);
        Task<string> CreateChatAsync(string userId);
    }

    public class GraphDelegatedService : IGraphDelegatedService
    {
        private readonly GraphServiceClient _graphServiceClient;
        private readonly ITokenService _tokenService;
        private readonly ILogger<GraphDelegatedService> _logger;
        private readonly string _serviceAccountUpn;

        public GraphDelegatedService(ITokenService tokenService, ILogger<GraphDelegatedService> logger)
        {
            _tokenService = tokenService;
            _logger = logger;

            var tokenProvider = new CustomTokenProvider(_tokenService);
            var authProvider = new BaseBearerTokenAuthenticationProvider(tokenProvider);
            _graphServiceClient = new GraphServiceClient(authProvider);
            _serviceAccountUpn = Environment.GetEnvironmentVariable("SERVICE_ACCOUNT_UPN");
        }

        public async Task<ChatMessage> SendChatMessageToUserAsync(string message, string chatId)
        {
            var requestBody = new ChatMessage
            {
                Body = new ItemBody { Content = message }
            };

            return await _graphServiceClient.Chats[chatId].Messages.PostAsync(requestBody);
        }

        public async Task<string> CreateChatAsync(string upn)
        {
            var requestBody = new Chat
            {
                ChatType = ChatType.OneOnOne,
                Members = new List<ConversationMember>
                {
                    new AadUserConversationMember
                    {
                        OdataType = "#microsoft.graph.aadUserConversationMember",
                        Roles = new List<string> { "owner" },
                        AdditionalData = new Dictionary<string, object>
                        {
                            { "user@odata.bind", $"https://graph.microsoft.com/v1.0/users('{_serviceAccountUpn}')" }
                        }
                    },
                    new AadUserConversationMember
                    {
                        OdataType = "#microsoft.graph.aadUserConversationMember",
                        Roles = new List<string> { "owner" },
                        AdditionalData = new Dictionary<string, object>
                        {
                            { "user@odata.bind", $"https://graph.microsoft.com/v1.0/users('{upn}')" }
                        }
                    }
                }
            };

            try
            {
                Chat result = await _graphServiceClient.Chats.PostAsync(requestBody);
                return result.Id;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error creating chat");
                return null;
            }
        }
    }

    public class CustomTokenProvider : IAccessTokenProvider
    {
        private readonly ITokenService _tokenService;

        public CustomTokenProvider(ITokenService tokenService)
        {
            _tokenService = tokenService;
        }

        public async Task<string> GetAuthorizationTokenAsync(Uri uri, Dictionary<string, object> additionalAuthenticationContext = null, CancellationToken cancellationToken = default)
        {
            return await _tokenService.GetAccessTokenAsync();
        }

        public AllowedHostsValidator AllowedHostsValidator { get; } = new AllowedHostsValidator();
    }
}