using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace groveale
{
    public class GetLeaderboard
    {
        private readonly ILogger<GetLeaderboard> _logger;

        public GetLeaderboard(ILogger<GetLeaderboard> logger)
        {
            _logger = logger;
        }

        [Function("GetLeaderboard")]
        public IActionResult Run([HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")] HttpRequest req)
        {
            _logger.LogInformation("C# HTTP trigger function processed a request.");
            return new OkObjectResult("Welcome to Azure Functions!");
        }
    }
}
