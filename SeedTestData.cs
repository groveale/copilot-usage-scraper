using groveale.Services;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

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

            try
            {
                // Seed daily activities for a tenant
                await _userActivitySeeder.SeedDailyActivitiesAsync("M365CPI7751573");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error seeding daily activities");
                return new BadRequestObjectResult("Error seeding daily activities");
            }

            return new OkObjectResult("Welcome to Azure Functions!");
        }
    }
}
