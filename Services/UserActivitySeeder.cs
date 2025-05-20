
using Microsoft.Extensions.Logging;

namespace groveale.Services
{
    public interface IUserActivitySeeder
    {
        Task SeedDailyActivitiesAsync(string tenantId);
        Task SeedWeeklyActivitiesAsync(string tenantId);
        Task SeedMonthlyActivitiesAsync(string tenantId);
        Task SeedAllTimeActivityAsync(string tenantId);
        Task SeedInactiveUsersAsync(string tenantId);
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

        public async Task SeedMonthlyActivitiesAsync(string tenantId)
        {
            _logger.LogInformation("Seeding monthly activities for tenant {tenantId}...", tenantId);

            var users = GetUsers(tenantId);

            // Get all users
            var random = new Random();
            var activities = new List<TimeFrameUsage>();

            foreach (var (upn, displayName) in users)
            {
                // Create a simple activity entry with random boolean values
                var activity = new TimeFrameUsage
                {
                    StartDate = DateTime.UtcNow.Date.AddDays(-1 * DateTime.UtcNow.Date.Day + 1),
                    UPN = upn,
                    DailyAllActivityCount = random.Next(20),
                    DailyTeamsActivityCount = random.Next(20),
                    DailyCopilotChatActivityCount = random.Next(20),
                    DailyOutlookActivityCount = random.Next(20),
                    DailyWordActivityCount = random.Next(20),
                    DailyExcelActivityCount = random.Next(20),
                    DailyPowerPointActivityCount = random.Next(20),
                    DailyOneNoteActivityCount = random.Next(20),
                    DailyLoopActivityCount = random.Next(20),
                    DailyMACActivityCount = random.Next(20),
                    DailyDesignerActivityCount = random.Next(20),
                    DailySharePointActivityCount = random.Next(20),
                    DailyPlannerActivityCount = random.Next(20),
                    DailyWhiteboardActivityCount = random.Next(20),
                    DailyStreamActivityCount = random.Next(20),
                    DailyFormsActivityCount = random.Next(20),
                    DailyCopilotActionActivityCount = random.Next(20),
                    DailyWebPluginActivityCount = random.Next(20),
                    TeamsInteractionCount = random.Next(200),
                    CopilotChatInteractionCount = random.Next(200),
                    OutlookInteractionCount = random.Next(200),
                    WordInteractionCount = random.Next(200),
                    ExcelInteractionCount = random.Next(200),
                    PowerPointInteractionCount = random.Next(200),
                    OneNoteInteractionCount = random.Next(200),
                    LoopInteractionCount = random.Next(200),
                    MACInteractionCount = random.Next(200),
                    DesignerInteractionCount = random.Next(200),
                    SharePointInteractionCount = random.Next(200),
                    PlannerInteractionCount = random.Next(200),
                    WhiteboardInteractionCount = random.Next(200),
                    StreamInteractionCount = random.Next(200),
                    FormsInteractionCount = random.Next(200),
                    CopilotActionInteractionCount = random.Next(200),
                    WebPluginInteractionCount = random.Next(200)
                };

                // Calculate DailyCopilotAllUpActivity, heightest of the other activities
                activity.DailyAllActivityCount = Math.Max(activity.DailyTeamsActivityCount, Math.Max(activity.DailyCopilotChatActivityCount, Math.Max(activity.DailyOutlookActivityCount, Math.Max(activity.DailyWordActivityCount, Math.Max(activity.DailyExcelActivityCount, Math.Max(activity.DailyPowerPointActivityCount, Math.Max(activity.DailyOneNoteActivityCount, activity.DailyLoopActivityCount)))))));
                activity.AllInteractionCount = activity.TeamsInteractionCount + activity.CopilotChatInteractionCount + activity.OutlookInteractionCount + activity.WordInteractionCount + activity.ExcelInteractionCount + activity.PowerPointInteractionCount + activity.OneNoteInteractionCount + activity.LoopInteractionCount + activity.MACInteractionCount + activity.DesignerInteractionCount + activity.SharePointInteractionCount + activity.PlannerInteractionCount + activity.WhiteboardInteractionCount + activity.StreamInteractionCount + activity.FormsInteractionCount + activity.CopilotActionInteractionCount + activity.WebPluginInteractionCount;

                activities.Add(activity);
            }

            // Save user activities
            await _storageSnapshotService.SeedTimeFrameActivitiesAsync(activities);

            _logger.LogInformation("Seeding monthly activities for tenant {tenantId} completed.", tenantId);
        }

        public async Task SeedWeeklyActivitiesAsync(string tenantId)
        {
            _logger.LogInformation("Seeding weekly activities for tenant {tenantId}...", tenantId);

            var users = GetUsers(tenantId);

            // Get all users
            var random = new Random();
            var activities = new List<TimeFrameUsage>();

            // Get the Monday of the current week
            var dayOfWeek = (int)DateTime.UtcNow.Date.DayOfWeek;
            var daysToSubtract = dayOfWeek == 0 ? 6 : dayOfWeek - 1; // Adjust for Sunday
            var monday = DateTime.UtcNow.Date.AddDays(-1 * daysToSubtract);

            foreach (var (upn, displayName) in users)
            {
                // Create a simple activity entry with random boolean values
                var activity = new TimeFrameUsage
                {
                    StartDate = monday,
                    UPN = upn,
                    DailyAllActivityCount = random.Next(5),
                    DailyTeamsActivityCount = random.Next(5),
                    DailyCopilotChatActivityCount = random.Next(5),
                    DailyOutlookActivityCount = random.Next(5),
                    DailyWordActivityCount = random.Next(5),
                    DailyExcelActivityCount = random.Next(5),
                    DailyPowerPointActivityCount = random.Next(5),
                    DailyOneNoteActivityCount = random.Next(5),
                    DailyLoopActivityCount = random.Next(5),
                    DailyMACActivityCount = random.Next(5),
                    DailyDesignerActivityCount = random.Next(5),
                    DailySharePointActivityCount = random.Next(5),
                    DailyPlannerActivityCount = random.Next(5),
                    DailyWhiteboardActivityCount = random.Next(5),
                    DailyStreamActivityCount = random.Next(5),
                    DailyFormsActivityCount = random.Next(5),
                    DailyCopilotActionActivityCount = random.Next(5),
                    DailyWebPluginActivityCount = random.Next(5),
                    TeamsInteractionCount = random.Next(50),
                    CopilotChatInteractionCount = random.Next(50),
                    OutlookInteractionCount = random.Next(50),
                    WordInteractionCount = random.Next(50),
                    ExcelInteractionCount = random.Next(50),
                    PowerPointInteractionCount = random.Next(50),
                    OneNoteInteractionCount = random.Next(50),
                    LoopInteractionCount = random.Next(50),
                    MACInteractionCount = random.Next(50),
                    DesignerInteractionCount = random.Next(50),
                    SharePointInteractionCount = random.Next(50),
                    PlannerInteractionCount = random.Next(50),
                    WhiteboardInteractionCount = random.Next(50),
                    StreamInteractionCount = random.Next(50),
                    FormsInteractionCount = random.Next(50),
                    CopilotActionInteractionCount = random.Next(50),
                    WebPluginInteractionCount = random.Next(50),
                    AllInteractionCount = random.Next(50)
                };

                activity.DailyAllActivityCount = Math.Max(activity.DailyTeamsActivityCount, Math.Max(activity.DailyCopilotChatActivityCount, Math.Max(activity.DailyOutlookActivityCount, Math.Max(activity.DailyWordActivityCount, Math.Max(activity.DailyExcelActivityCount, Math.Max(activity.DailyPowerPointActivityCount, Math.Max(activity.DailyOneNoteActivityCount, activity.DailyLoopActivityCount)))))));
                // sum all the interaction counts
                activity.AllInteractionCount = activity.TeamsInteractionCount + activity.CopilotChatInteractionCount + activity.OutlookInteractionCount + activity.WordInteractionCount + activity.ExcelInteractionCount + activity.PowerPointInteractionCount + activity.OneNoteInteractionCount + activity.LoopInteractionCount + activity.MACInteractionCount + activity.DesignerInteractionCount + activity.SharePointInteractionCount + activity.PlannerInteractionCount + activity.WhiteboardInteractionCount + activity.StreamInteractionCount + activity.FormsInteractionCount + activity.CopilotActionInteractionCount + activity.WebPluginInteractionCount;

                activities.Add(activity);
            }

            // Save user activities
            await _storageSnapshotService.SeedTimeFrameActivitiesAsync(activities);

            _logger.LogInformation("Seeding weekly activities for tenant {tenantId} completed.", tenantId);
        }

        public async Task SeedAllTimeActivityAsync(string tenantId)
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
                        UPN = upn,
                        App = app,
                        TotalDailyActivityCount = random.Next(100),
                        BestDailyStreak = random.Next(30),
                        CurrentDailyStreak = random.Next(30)
                    };

                    // add
                    activities.Add(activity);
                }
            }

            // Save user activities
            await _storageSnapshotService.SeedAllTimeActivityAsync(activities);

            _logger.LogInformation("Seeding all time activities for tenant {tenantId} completed.", tenantId);
        }

        public async Task SeedInactiveUsersAsync(string tenantId)
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
