using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace groveale
{
    public class UpdateRefreshTokenFromAccessCode
    {
        private readonly ILogger<UpdateRefreshTokenFromAccessCode> _logger;
        private readonly ITokenService _tokenService;

        public UpdateRefreshTokenFromAccessCode(ILogger<UpdateRefreshTokenFromAccessCode> logger, ITokenService tokenService)
        {
            _logger = logger;
            _tokenService = tokenService;
        }

        [Function("UpdateRefreshTokenFromAccessCode")]
        public async Task<IActionResult> RunAsync([HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")] HttpRequest req)
        {
            _logger.LogInformation("C# HTTP trigger function processed a request.");

            string code = req.Query["code"];

            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            dynamic data = JsonConvert.DeserializeObject(requestBody);
            code = code ?? data?.code;


            try
            {
                if (string.IsNullOrEmpty(code))
                {
                    return new BadRequestObjectResult("Please pass a code on the query string or in the request body");
                }

                var accessToken = await _tokenService.GetRefreshTokenFromCodeAsync(code);

                return new OkObjectResult(new { accessToken });
            }
            catch (Exception ex)
            {
                return new BadRequestObjectResult(ex.Message);
            }
        }
    }
}
