
using Microsoft.Extensions.Logging;

namespace groveale.Services
{
    public interface IUserActivitySeeder
    {
        Task SeedDailyActivitiesAsync(string tenantId);
        Task SeedWeeklyActivitiesAsync(string tenantId);
        Task SeedMonthlyActivitiesAsync(string tenantId);
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

            var users = GetUsers(tenantId);

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

        public async Task SeedMonthlyActivitiesAsync(string tenantId)
        {
            _logger.LogInformation("Seeding monthly activities for tenant {tenantId}...", tenantId);

            var users = GetUsers(tenantId);

            // Get all users
            var random = new Random();
            var activities = new List<MonthlyUsage>();

            foreach (var (upn, displayName) in users)
            {
                // Create a simple activity entry with random boolean values
                var activity = new MonthlyUsage
                {
                    StartDate = DateTime.UtcNow.Date.AddDays(-1 * DateTime.UtcNow.Date.Day + 1),
                    UPN = upn,
                    DisplayName = displayName,
                    DailyTeamsActivityCount = random.Next(20),
                    DailyCopilotChatActivityCount = random.Next(20),
                    DailyOutlookActivityCount = random.Next(20),
                    DailyWordActivityCount = random.Next(20),
                    DailyExcelActivityCount = random.Next(20),
                    DailyPowerPointActivityCount = random.Next(20),
                    DailyOneNoteActivityCount = random.Next(20),
                    DailyLoopActivityCount = random.Next(20)
                };

                // Calculate DailyCopilotAllUpActivity, heightest of the other activities
                activity.DailyAllActivityCount = Math.Max(activity.DailyTeamsActivityCount, Math.Max(activity.DailyCopilotChatActivityCount, Math.Max(activity.DailyOutlookActivityCount, Math.Max(activity.DailyWordActivityCount, Math.Max(activity.DailyExcelActivityCount, Math.Max(activity.DailyPowerPointActivityCount, Math.Max(activity.DailyOneNoteActivityCount, activity.DailyLoopActivityCount)))))));
                activities.Add(activity);
            }

            // Save user activities
            await _storageSnapshotService.SeedMonthlyActivitiesAsync(activities);

            _logger.LogInformation("Seeding monthly activities for tenant {tenantId} completed.", tenantId);
        }

        public async Task SeedWeeklyActivitiesAsync(string tenantId)
        {
            _logger.LogInformation("Seeding weekly activities for tenant {tenantId}...", tenantId);

            var users = GetUsers(tenantId);

            // Get all users
            var random = new Random();
            var activities = new List<WeeklyUsage>();

            // Get the Monday of the current week
            var dayOfWeek = (int)DateTime.UtcNow.Date.DayOfWeek;
            var daysToSubtract = dayOfWeek == 0 ? 6 : dayOfWeek - 1; // Adjust for Sunday
            var monday = DateTime.UtcNow.Date.AddDays(-1 * daysToSubtract);

            foreach (var (upn, displayName) in users)
            {
                // Create a simple activity entry with random boolean values
                var activity = new WeeklyUsage
                {
                    StartDate = monday,
                    UPN = upn,
                    DisplayName = displayName,
                    DailyTeamsActivityCount = random.Next(5),
                    DailyCopilotChatActivityCount = random.Next(5),
                    DailyOutlookActivityCount = random.Next(5),
                    DailyWordActivityCount = random.Next(5),
                    DailyExcelActivityCount = random.Next(5),
                    DailyPowerPointActivityCount = random.Next(5),
                    DailyOneNoteActivityCount = random.Next(5),
                    DailyLoopActivityCount = random.Next(5)
                };

                // Calculate DailyCopilotAllUpActivity, heightest of the other activities
                activity.DailyAllActivityCount = Math.Max(activity.DailyTeamsActivityCount, Math.Max(activity.DailyCopilotChatActivityCount, Math.Max(activity.DailyOutlookActivityCount, Math.Max(activity.DailyWordActivityCount, Math.Max(activity.DailyExcelActivityCount, Math.Max(activity.DailyPowerPointActivityCount, Math.Max(activity.DailyOneNoteActivityCount, activity.DailyLoopActivityCount)))))));
                activities.Add(activity);
            }

            // Save user activities
            await _storageSnapshotService.SeedWeeklyActivitiesAsync(activities);

            _logger.LogInformation("Seeding weekly activities for tenant {tenantId} completed.", tenantId);
        }

        private List<(string upn, string displayName)> GetUsers(string tenantId) =>
            new List<(string upn, string displayName)>
            {
                ($"DakotaS@{tenantId}.OnMicrosoft.com", "Dakota Sanchez"),
                ($"EkaS@{tenantId}.OnMicrosoft.com", "Eka Siahaan"),
                ($"HadarC@{tenantId}.OnMicrosoft.com", "Hadar Caspit"),
                ($"KaiC@{tenantId}.OnMicrosoft.com", "Kai Carter"),
                ($"LisaT@{tenantId}.OnMicrosoft.com", "Lisa Taylor"),
                ($"MarioR@{tenantId}.OnMicrosoft.com", "Mario Rogers"),
                ($"MonicaT@{tenantId}.OnMicrosoft.com", "Monica Thompson")
            };
    }
}
