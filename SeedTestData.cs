using groveale.Services;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace groveale
{
    public class SeedTestData
    {
        private readonly ILogger<SeedTestData> _logger;
        private readonly IUserActivitySeeder _userActivitySeeder;

        public SeedTestData(ILogger<SeedTestData> logger, IUserActivitySeeder userActivitySeeder)
        {
            _logger = logger;
            _userActivitySeeder = userActivitySeeder;
        }

        [Function("SeedTestData")]
        public async Task<IActionResult> Run([HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")] HttpRequest req)
        {
            _logger.LogInformation("C# HTTP trigger function processed a request.");

            // get tenantId from query string
            string tenantId = req.Query["tenantId"];

            // also check in the body
            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            dynamic data = JsonConvert.DeserializeObject(requestBody);
            tenantId = tenantId ?? data?.tenantId;

            if (string.IsNullOrEmpty(tenantId))
            {
                return new BadRequestObjectResult("Please pass a tenantId on the query string or in the request body");
            }

            try
            {
                // Seed daily activities for a tenant
                await _userActivitySeeder.SeedDailyActivitiesAsync(tenantId);
                await _userActivitySeeder.SeedWeeklyActivitiesAsync(tenantId);
                await _userActivitySeeder.SeedMonthlyActivitiesAsync(tenantId);
                
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error seeding activities");
                return new BadRequestObjectResult("Error seeding activities");
            }

            return new OkObjectResult($"Data seeded successfully for tenant {tenantId}");
        }
    }
}
