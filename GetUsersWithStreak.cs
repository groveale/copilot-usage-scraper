using groveale.Services;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace groveale
{
    public class GetUsersWithStreak
    {
        private readonly ILogger<GetUsersWithStreak> _logger;
        private readonly ICopilotUsageSnapshotService _copilotUsageSnapshotService;

        public GetUsersWithStreak(ILogger<GetUsersWithStreak> logger, ICopilotUsageSnapshotService copilotUsageSnapshotService)
        {
            _logger = logger;
            _copilotUsageSnapshotService = copilotUsageSnapshotService;
        }

        [Function("GetUsersWithStreak")]
        public async Task<IActionResult> Run([HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")] HttpRequest req)
        {
            _logger.LogInformation("C# HTTP trigger function processed a request.");

            // Parse query parameter
            string count = req.Query["count"];

            // Parse request body
            string requestBody = new StreamReader(req.Body).ReadToEndAsync().Result;
            dynamic data = Newtonsoft.Json.JsonConvert.DeserializeObject(requestBody);
            count = count ?? data?.count;

            // Handle app parameter as an array or a comma-separated string
            List<string> appList;
            if (data?.apps is Newtonsoft.Json.Linq.JArray)
            {
                appList = data.apps.ToObject<List<string>>();
            }
            else
            {
                string apps = req.Query["apps"];
                appList = apps?.Split(',').Select(a => a.Trim()).ToList();
            }

            // Validate params
            if (appList == null || !appList.Any() || string.IsNullOrEmpty(count))
            {
                return new BadRequestObjectResult("Please pass an 'apps' list and count parameter on the query string or body");
            }

            if (!int.TryParse(count, out int countValue))
            {
                return new BadRequestObjectResult("Please pass a valid integer for the count parameter");
            }

            try
            {
                // Get users with streak
                var users = await _copilotUsageSnapshotService.GetUsersWithStreak(appList, countValue);

                return new OkObjectResult(users);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "An error occurred while processing the request.");
                return new StatusCodeResult(StatusCodes.Status500InternalServerError);
            }
           

        }
    }
}
