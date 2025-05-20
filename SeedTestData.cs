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
        private readonly ISettingsService _settingsService;
        private readonly IKeyVaultService _keyVaultService;

        public SeedTestData(ILogger<SeedTestData> logger, IUserActivitySeeder userActivitySeeder, ISettingsService settingsService, IKeyVaultService keyVaultService)
        {
            _settingsService = settingsService;
            _keyVaultService = keyVaultService;
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
            
            // Create Encyption Service
            var encryptionService = await DeterministicEncryptionService.CreateAsync(_settingsService, _keyVaultService);

            try
            {
                // Seed daily activities for a tenant
                await _userActivitySeeder.SeedDailyActivitiesAsync(tenantId, encryptionService);
                await _userActivitySeeder.SeedWeeklyActivitiesAsync(tenantId, encryptionService);
                await _userActivitySeeder.SeedMonthlyActivitiesAsync(tenantId, encryptionService);
                await _userActivitySeeder.SeedAllTimeActivityAsync(tenantId, encryptionService);
                await _userActivitySeeder.SeedInactiveUsersAsync(tenantId, encryptionService);

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
