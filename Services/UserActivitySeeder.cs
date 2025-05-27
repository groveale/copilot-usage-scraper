
using Microsoft.Extensions.Logging;

namespace groveale.Services
{
    public interface IUserActivitySeeder
    {
        Task SeedDailyActivitiesAsync(string tenantId, IDeterministicEncryptionService encryptionService);
        Task SeedWeeklyActivitiesAsync(string tenantId, IDeterministicEncryptionService encryptionService);
        Task SeedMonthlyActivitiesAsync(string tenantId, IDeterministicEncryptionService encryptionService);
        Task SeedAllTimeActivityAsync(string tenantId, IDeterministicEncryptionService encryptionService);
        Task SeedInactiveUsersAsync(string tenantId, IDeterministicEncryptionService encryptionService);
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

        public async Task SeedDailyActivitiesAsync(string tenantId, IDeterministicEncryptionService encryptionService)
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
                    DailyTeamsInteractionCount = random.Next(50),
                    DailyCopilotChatActivity = random.Next(2) == 1,
                    DailyCopilotChatInteractionCount = random.Next(50),
                    DailyOutlookActivity = random.Next(2) == 1,
                    DailyOutlookInteractionCount = random.Next(50),
                    DailyWordActivity = random.Next(2) == 1,
                    DailyWordInteractionCount = random.Next(50),
                    DailyExcelActivity = random.Next(2) == 1,
                    DailyExcelInteractionCount = random.Next(50),
                    DailyPowerPointActivity = random.Next(2) == 1,
                    DailyPowerPointInteractionCount = random.Next(50),
                    DailyOneNoteActivity = random.Next(2) == 1,
                    DailyOneNoteInteractionCount = random.Next(50),
                    DailyLoopActivity = random.Next(2) == 1,
                    DailyLoopInteractionCount = random.Next(50),
                    DailyAllUpActivity = random.Next(2) == 1,
                    DailyAllInteractionCount = random.Next(50),
                    DailyMACActivity = random.Next(2) == 1,
                    DailyMACInteractionCount = random.Next(50),
                    DailyDesignerActivity = random.Next(2) == 1,
                    DailyDesignerInteractionCount = random.Next(50),
                    DailySharePointActivity = random.Next(2) == 1,
                    DailySharePointInteractionCount = random.Next(50),
                    DailyPlannerActivity = random.Next(2) == 1,
                    DailyPlannerInteractionCount = random.Next(50),
                    DailyWhiteboardActivity = random.Next(2) == 1,
                    DailyWhiteboardInteractionCount = random.Next(50),
                    DailyStreamActivity = random.Next(2) == 1,
                    DailyStreamInteractionCount = random.Next(50),
                    DailyFormsActivity = random.Next(2) == 1,
                    DailyFormsInteractionCount = random.Next(50),
                    DailyCopilotActionActivity = random.Next(2) == 1,
                    DailyCopilotActionCount = random.Next(50),
                    DailyWebPluginActivity = random.Next(2) == 1,
                    DailyWebPluginInteractions = random.Next(50)
                };

                // Calculate DailyCopilotAllUpActivity if any of the other activities are true
                activity.DailyAllUpActivity = activity.DailyTeamsActivity || activity.DailyCopilotChatActivity || activity.DailyOutlookActivity || activity.DailyWordActivity || activity.DailyExcelActivity || activity.DailyPowerPointActivity || activity.DailyOneNoteActivity || activity.DailyLoopActivity;

                activities.Add(activity);
            }

            // Save user activities
            await _storageSnapshotService.SeedDailyActivitiesAsync(activities);

            _logger.LogInformation("Seeding daily activities for tenant {tenantId} completed.", tenantId);
        }

        public async Task SeedMonthlyActivitiesAsync(string tenantId, IDeterministicEncryptionService encryptionService)
        {
            _logger.LogInformation("Seeding monthly activities for tenant {tenantId}...", tenantId);

            var users = GetUsers(tenantId);

            // Get all users
            var random = new Random();

            var activities = new List<CopilotTimeFrameUsage>();

            var firstOfMonthForSnapshot = DateTime.UtcNow.Date
                .AddDays(-1 * DateTime.UtcNow.Date.Day + 1)
                .ToString("yyyy-MM-dd");

            foreach (var (upn, displayName) in users)
            {
                foreach (var app in Enum.GetValues(typeof(AppType)).Cast<AppType>())
                {
                    // Create a simple activity entry with random boolean values
                    var activity = new CopilotTimeFrameUsage
                    {
                        UPN = encryptionService.Encrypt(upn),
                        App = app,
                        TotalDailyActivityCount = random.Next(30),
                        TotalInteractionCount = random.Next(1000),
                        BestDailyStreak = random.Next(30),
                        CurrentDailyStreak = random.Next(30)
                    };

                    // add
                    activities.Add(activity);
                }
            }

            // Save user activities
            await _storageSnapshotService.SeedMonthlyFrameActivitiesAsync(activities, firstOfMonthForSnapshot);

            _logger.LogInformation("Seeding monthly activities for tenant {tenantId} completed.", tenantId);
        }

        public async Task SeedWeeklyActivitiesAsync(string tenantId, IDeterministicEncryptionService encryptionService)
        {
            _logger.LogInformation("Seeding weekly activities for tenant {tenantId}...", tenantId);

            var users = GetUsers(tenantId);

            // Get all users
            var random = new Random();


            // Get the Monday of the current week
            var dayOfWeek = (int)DateTime.UtcNow.Date.DayOfWeek;
            var daysToSubtract = dayOfWeek == 0 ? 6 : dayOfWeek - 1; // Adjust for Sunday
            var monday = DateTime.UtcNow.Date.AddDays(-1 * daysToSubtract);

            var activities = new List<CopilotTimeFrameUsage>();

            foreach (var (upn, displayName) in users)
            {
                foreach (var app in Enum.GetValues(typeof(AppType)).Cast<AppType>())
                {
                    // Create a simple activity entry with random boolean values
                    var activity = new CopilotTimeFrameUsage
                    {
                        UPN = encryptionService.Encrypt(upn),
                        App = app,
                        TotalDailyActivityCount = random.Next(5),
                        TotalInteractionCount = random.Next(100),
                        BestDailyStreak = random.Next(5),
                        CurrentDailyStreak = random.Next(5)
                    };

                    // add
                    activities.Add(activity);
                }
            }

            // Save user activities
            await _storageSnapshotService.SeedWeeklyTimeFrameActivitiesAsync(activities, monday.ToString("yyyy-MM-dd"));

            _logger.LogInformation("Seeding weekly activities for tenant {tenantId} completed.", tenantId);
        }

        public async Task SeedAllTimeActivityAsync(string tenantId, IDeterministicEncryptionService encryptionService)
        {
            _logger.LogInformation("Seeding all time activities for tenant {tenantId}...", tenantId);

            var users = GetUsers(tenantId);

            // Get all users
            var random = new Random();
            var activities = new List<CopilotTimeFrameUsage>();

            foreach (var (upn, displayName) in users)
            {
                foreach (var app in Enum.GetValues(typeof(AppType)).Cast<AppType>())
                {
                    // Create a simple activity entry with random boolean values
                    var activity = new CopilotTimeFrameUsage
                    {
                        UPN = encryptionService.Encrypt(upn),
                        App = app,
                        TotalInteractionCount = random.Next(5000),
                        TotalDailyActivityCount = random.Next(100),
                        BestDailyStreak = random.Next(100),
                        CurrentDailyStreak = random.Next(100)
                    };

                    // add
                    activities.Add(activity);
                }
            }

            // Save user activities
            await _storageSnapshotService.SeedAllTimeActivityAsync(activities);

            _logger.LogInformation("Seeding all time activities for tenant {tenantId} completed.", tenantId);
        }

        public async Task SeedInactiveUsersAsync(string tenantId, IDeterministicEncryptionService encryptionService)
        {
            _logger.LogInformation("Seeding inactive users for tenant {tenantId}...", tenantId);

            var users = GetUsers(tenantId);

            // Get all users
            var random = new Random();
            var activities = new List<InactiveUser>();

            int[] possibleDays = { 7, 14, 30, 60, 90 };

            // Today
            var today = DateTime.UtcNow.Date;

            foreach (var (upn, displayName) in users)
            {
                // Create a simple activity entry with random boolean values
                var activity = new InactiveUser
                {
                    UPN = upn,
                    DaysSinceLastActivity = possibleDays[random.Next(0, possibleDays.Length)],
                    DisplayName = displayName,

                };

                // Calculate LastActivityDate based on DaysSinceLastActivity
                activity.LastActivityDate = today.AddDays(-activity.DaysSinceLastActivity);

                activities.Add(activity);
            }

            // Save user activities
            await _storageSnapshotService.SeedInactiveUsersAsync(activities);

            _logger.LogInformation("Seeding inactive users for tenant {tenantId} completed.", tenantId);
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
                ($"MonicaT@{tenantId}.OnMicrosoft.com", "Monica Thompson"),
                ($"admin@{tenantId}.onmicrosoft.com", "MOD Administrator")
            };
    }
}
