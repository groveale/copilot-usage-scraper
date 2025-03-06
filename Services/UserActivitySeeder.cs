
using Microsoft.Extensions.Logging;

namespace groveale.Services
{
    public interface IUserActivitySeeder
    {
        Task SeedDailyActivitiesAsync(string tenantId);
    }

    public class UserActivitySeeder : IUserActivitySeeder
    {
        private readonly ILogger<UserActivitySeeder> _logger;
        private readonly ICopilotUsageSnapshotService _storageSnapshotService;
        private readonly ISettingsService _settingsService;

        public UserActivitySeeder(ILogger<UserActivitySeeder> logger, ICopilotUsageSnapshotService storageSnapshotService, ISettingsService settingsService)
        {
            _logger = logger;
            _storageSnapshotService = storageSnapshotService;
            _settingsService = settingsService;
        }

        public async Task SeedDailyActivitiesAsync(string tenantId)
        {
            _logger.LogInformation("Seeding daily activities for tenant {tenantId}...", tenantId);

            var users = new List<(string upn, string displayName)>
            {
                ($"DakotaS@{tenantId}.OnMicrosoft.com", "Dakota Sanchez"),
                ($"EkaS@{tenantId}.OnMicrosoft.com", "Eka Siahaan"),
                ($"HadarC@{tenantId}.OnMicrosoft.com", "Hadar Caspit"),
                ($"KaiC@{tenantId}.OnMicrosoft.com", "Kai Carter"),
                ($"LisaT@{tenantId}CPI7751573.OnMicrosoft.com", "Lisa Taylor"),
                ($"MarioR@{tenantId}.OnMicrosoft.com", "Mario Rogers"),
                ($"MonicaT@{tenantId}.OnMicrosoft.com", "Monica Thompson")
            };

            // Get all users
            var random = new Random();
            var activities = new List<UserActivity>();
            
            foreach (var (upn, displayName) in users)
            {
                // Create a simple activity entry with random boolean values
                var activity = new UserActivity
                {
                    ReportDate = DateTime.UtcNow.Date,
                    UPN = upn,
                    DisplayName = displayName,
                    DailyTeamsActivity = random.Next(2) == 1,
                    DailyCopilotChatActivity = random.Next(2) == 1,
                    DailyOutlookActivity = random.Next(2) == 1,
                    DailyWordActivity = random.Next(2) == 1,
                    DailyExcelActivity = random.Next(2) == 1,
                    DailyPowerPointActivity = random.Next(2) == 1,
                    DailyOneNoteActivity = random.Next(2) == 1,
                    DailyLoopActivity = random.Next(2) == 1
                };
                
                // Calculate DailyCopilotAllUpActivity if any of the other activities are true
                activity.DailyCopilotAllUpActivity = activity.DailyTeamsActivity || activity.DailyCopilotChatActivity || activity.DailyOutlookActivity || activity.DailyWordActivity || activity.DailyExcelActivity || activity.DailyPowerPointActivity || activity.DailyOneNoteActivity || activity.DailyLoopActivity;
                
                activities.Add(activity);
            }

            // Save user activities
            await _storageSnapshotService.SeedDailyActivitiesAsync(activities);

            _logger.LogInformation("Seeding daily activities for tenant {tenantId} completed.", tenantId);
        }
    }
    }
