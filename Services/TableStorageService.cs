using System.Text.Json;
using Azure;
using Azure.Data.Tables;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Graph.Beta.Models;

namespace groveale.Services
{
    public interface ICopilotUsageSnapshotService
    {
        Task<int> ProcessUserDailySnapshots(List<M365CopilotUsage> siteSnapshots);

        Task<List<CopilotReminderItem>> GetUsersForQueue();

        Task UpdateUserWeeklySnapshots(M365CopilotUsage dailySnapshots);
        Task UpdateUserMonthlySnapshots(M365CopilotUsage dailySnapshots);
        Task UpdateUserAllTimeSnapshots(M365CopilotUsage dailySnapshots);

        Task ResetUsersAppStreak(AppType appType, string upn);

        Task<string?> GetStartDate(string timeFrame);

        Task<List<string>> GetUsersWhoHaveCompletedActivity(List<string> apps, string count, string timeFrame, string startDate);
        Task<List<string>> GetUsersWhoHaveCompletedActivity(string app, string count, string timeFrame, string startDate);

        // For Seeding
        Task SeedDailyActivitiesAsync(List<UserActivity> userActivitiesSeed);
        Task SeedWeeklyActivitiesAsync(List<WeeklyUsage> userActivitiesSeed);
        Task SeedMonthlyActivitiesAsync(List<MonthlyUsage> userActivitiesSeed);
    }

    public class CopilotUsageSnapshotService : ICopilotUsageSnapshotService
    {
        private readonly TableServiceClient _serviceClient;
        private readonly TableClient _userDAUTableClient;
        private readonly TableClient _userLastUsageTableClient;
        private readonly TableClient _userWeeklyTableClient;
        private readonly TableClient _userMonthlyTableClient;
        private readonly TableClient _userAllTimeTableClient;
        private readonly TableClient _reportRefreshDateTableClient;
        private readonly ILogger<CopilotUsageSnapshotService> _logger;
        private readonly bool CDXTenant = System.Environment.GetEnvironmentVariable("CDXTenant") == "true";
        private readonly string _userDAUTableName = "CopilotUsageDailySnapshots";
        private readonly string _userLastUsageTableName = "UsersLastUsageTracker";
        private readonly string _userWeeklyTableName = "CopilotUsageWeeklySnapshots";
        private readonly string _userMonthlyTableName = "CopilotUsageMonthlySnapshots";
        private readonly string _userAllTimeTableName = "CopilotUsageAllTimeRecord";
        private readonly string _reportRefreshDateTableName = "ReportRefreshRecord";

        private readonly int _daysToCheck = int.TryParse(System.Environment.GetEnvironmentVariable("ReminderDays"), out var days) ? days : 0;

        private readonly int reminderInterval = int.TryParse(System.Environment.GetEnvironmentVariable("ReminderInterval"), out var date) ? date : 0;

        private readonly int reminderCount = int.TryParse(System.Environment.GetEnvironmentVariable("ReminderCount"), out var date) ? date : 0;

        public CopilotUsageSnapshotService(ILogger<CopilotUsageSnapshotService> logger)
        {
            _logger = logger;

            var storageUri = System.Environment.GetEnvironmentVariable("StorageAccountUri");
            var accountName = System.Environment.GetEnvironmentVariable("StorageAccountName");
            var storageAccountKey = System.Environment.GetEnvironmentVariable("StorageAccountKey");


            _serviceClient = new TableServiceClient(
                new Uri(storageUri),
                new TableSharedKeyCredential(accountName, storageAccountKey));

            _userDAUTableClient = _serviceClient.GetTableClient(_userDAUTableName);
            _userDAUTableClient.CreateIfNotExists();

            _userLastUsageTableClient = _serviceClient.GetTableClient(_userLastUsageTableName);
            _userLastUsageTableClient.CreateIfNotExists();

            _userWeeklyTableClient = _serviceClient.GetTableClient(_userWeeklyTableName);
            _userWeeklyTableClient.CreateIfNotExists();

            _userMonthlyTableClient = _serviceClient.GetTableClient(_userMonthlyTableName);
            _userMonthlyTableClient.CreateIfNotExists();

            _userAllTimeTableClient = _serviceClient.GetTableClient(_userAllTimeTableName);
            _userAllTimeTableClient.CreateIfNotExists();

            _reportRefreshDateTableClient = _serviceClient.GetTableClient(_reportRefreshDateTableName);
            _reportRefreshDateTableClient.CreateIfNotExists();
        }

        public async Task<List<CopilotReminderItem>> GetUsersForQueue()
        {
            // Get users from the DB that need a notification
            var tableClient = _serviceClient.GetTableClient(_userLastUsageTableName);
            tableClient.CreateIfNotExists();

            //


            // Define the query filter
            // todo need to also add days since usage (all users will be here)
            string filter = TableClient.CreateQueryFilter(
                $"(DaysSinceLastNotification gt {reminderInterval}) and (NotificationCount lt {reminderCount} or NotificationCount eq 0)"
            );

            _logger.LogInformation($"Filter: {filter}");

            var records = new List<CopilotReminderItem>();

            try
            {
                // Query all records with filter
                AsyncPageable<TableEntity> queryResults = tableClient.QueryAsync<TableEntity>(filter);



                await foreach (TableEntity entity in queryResults)
                {
                    records.Add(new CopilotReminderItem
                    {
                        UPN = entity.PartitionKey,
                        DisplayName = entity.GetString("DisplayName"),
                        LastActivityDate = entity.GetString("LastActivityDate"),
                        DaysSinceLastActivity = entity.GetDouble("DaysSinceLastActivity") ?? 0,
                        DaysSinceLastNotification = entity.GetDouble("DaysSinceLastNotification") ?? 0,
                        NotificationCount = entity.GetInt32("NotificationCount") ?? 0

                    });

                }
            }
            catch (RequestFailedException ex)
            {
                Console.WriteLine($"Error retrieving records: {ex.Message}");
                throw;
            }

            return records;

        }

        public async Task<int> ProcessUserDailySnapshots(List<M365CopilotUsage> userSnapshots)
        {
            int DAUadded = 0;

            // Tuple to store the user's last activity date and username
            var lastActivityDates = new List<(string, string, string, string)>();

            string reportRefreshDateString = userSnapshots[0].ReportRefreshDate;

            foreach (var userSnap in userSnapshots)
            {
                // if last activity date is not the same as the report refresh date, we need to validate how long usage has not occured
                // there is no daily activity if the last activity date is not the same as the report refresh date

                if (userSnap.LastActivityDate != userSnap.ReportRefreshDate)
                {
                    var reportRefreshDate = DateTime.ParseExact(userSnap.ReportRefreshDate, "yyyy-MM-dd", null);

                    if (string.IsNullOrEmpty(userSnap.LastActivityDate))
                    {
                        // we need to record in another table
                        // Can we set the last activity as epoch when null?
                        var epochTime = new DateTime(1970, 1, 1);
                        lastActivityDates.Add((epochTime.ToString("yyyy-MM-dd"), userSnap.UserPrincipalName, userSnap.ReportRefreshDate, userSnap.DisplayName));
                    }
                    else
                    {
                        // Convert to date time
                        var lastActivityDate = DateTime.ParseExact(userSnap.LastActivityDate, "yyyy-MM-dd", null);


                        // Check if last activity is before days ti check
                        if (lastActivityDate.AddDays(_daysToCheck) < reportRefreshDate)
                        {
                            // we need to record in another table
                            lastActivityDates.Add((userSnap.LastActivityDate, userSnap.UserPrincipalName, userSnap.ReportRefreshDate, userSnap.DisplayName));
                        }
                    }

                    // Todo: Reset alltime snapshots
                    // Check if ReportRefreshDate is not a weekend
                    if (reportRefreshDate.DayOfWeek != DayOfWeek.Saturday && reportRefreshDate.DayOfWeek != DayOfWeek.Sunday)
                    {
                        // reset streaks for all app as no daily usage
                    }

                }

                var userEntity = ConvertToUserActivity(userSnap);

                var tableEntity = new TableEntity(userEntity.ReportDate.ToString("yyyy-MM-dd"), userEntity.UPN)
                {
                    { "ReportDate", userEntity.ReportDate },
                    { "DisplayName", userEntity.DisplayName },
                    { "DailyTeamsActivity", userEntity.DailyTeamsActivity },
                    { "DailyOutlookActivity", userEntity.DailyOutlookActivity },
                    { "DailyWordActivity", userEntity.DailyWordActivity },
                    { "DailyExcelActivity", userEntity.DailyExcelActivity },
                    { "DailyPowerPointActivity", userEntity.DailyPowerPointActivity },
                    { "DailyOneNoteActivity", userEntity.DailyOneNoteActivity },
                    { "DailyLoopActivity", userEntity.DailyLoopActivity },
                    { "DailyCopilotChatActivity", userEntity.DailyCopilotChatActivity },
                    { "DailyAllActivity", userEntity.DailyCopilotAllUpActivity }
                };

                try
                {
                    // Try to add the entity if it doesn't exist
                    await _userDAUTableClient.AddEntityAsync(tableEntity);
                    DAUadded++;

                }
                catch (Azure.RequestFailedException ex) when (ex.Status == 409) // Conflict indicates the entity already exists
                {
                    // Merge the entity if it already exists
                    await _userDAUTableClient.UpdateEntityAsync(tableEntity, ETag.All, TableUpdateMode.Merge);
                }

                // We need to update the last weekly, monthly and alltime tables
                await UpdateUserAllTimeSnapshots(userSnap);
                await UpdateUserWeeklySnapshots(userSnap);
                await UpdateUserMonthlySnapshots(userSnap);


            }

            // Update the timeFrame table
            await UpdateReportRefreshDate(reportRefreshDateString, "daily");
            await UpdateReportRefreshDate(reportRefreshDateString, "weekly");
            await UpdateReportRefreshDate(reportRefreshDateString, "monthly");

            // For notifications - now handled elsewhere
            return DAUadded;

            // Do we have any users to record in the last activity table?
            if (lastActivityDates.Count > 0)
            {

                var today = DateTime.UtcNow.ToString("yyyy-MM-dd");

                foreach (var (lastActivityDate, userPrincipalName, reportRefreshDateItem, displayName) in lastActivityDates)
                {

                    // Check if record exists in table for user
                    var tableEntity = new TableEntity(userPrincipalName, lastActivityDate)
                    {
                        { "LastActivityDate", lastActivityDate },
                        { "ReportRefreshDate", reportRefreshDateItem },
                        { "DaysSinceLastActivity", (DateTime.ParseExact(reportRefreshDateItem, "yyyy-MM-dd", null) - DateTime.ParseExact(lastActivityDate, "yyyy-MM-dd", null)).TotalDays },
                        { "LastNotificationDate", today },
                        { "DaysSinceLastNotification", (double)999 },
                        { "NotificationCount", 0 },
                        { "DisplayName", displayName }
                    };

                    try
                    {
                        // Try to add the entity if it doesn't exist
                        await _userLastUsageTableClient.AddEntityAsync(tableEntity);
                    }
                    catch (Azure.RequestFailedException ex) when (ex.Status == 409) // Conflict indicates the entity already exists
                    {
                        // Merge the entity if it already exists
                        // Get the existing entity
                        var existingTableEntity = await _userLastUsageTableClient.GetEntityAsync<TableEntity>(userPrincipalName, lastActivityDate);

                        // persit the existing value for LastNotificationDate and NotificationCount
                        tableEntity["LastNotificationDate"] = existingTableEntity.Value["LastNotificationDate"];

                        if (existingTableEntity.Value.GetInt32("NotificationCount") != 0)
                        {
                            tableEntity["NotificationCount"] = existingTableEntity.Value["NotificationCount"];
                        }
                        else
                        {
                            tableEntity["NotificationCount"] = 1;
                        }

                        // Need to up the days since last notification
                        tableEntity["DaysSinceLastNotification"] = (DateTime.ParseExact(today, "yyyy-MM-dd", null) - DateTime.ParseExact(existingTableEntity.Value["LastNotificationDate"].ToString(), "yyyy-MM-dd", null)).TotalDays;

                        // check if we need to send a reminder
                        var daysSinceLastNotification = tableEntity.GetDouble("DaysSinceLastNotification") ?? 0;

                        if (daysSinceLastNotification >= reminderInterval)
                        {
                            // Add to the notification count
                            tableEntity["NotificationCount"] = (existingTableEntity.Value.GetInt32("NotificationCount") ?? 0) + 1;

                            // Add to the last notification date
                            tableEntity["LastNotificationDate"] = today;
                        }

                        await _userLastUsageTableClient.UpdateEntityAsync(tableEntity, ETag.All, TableUpdateMode.Merge);
                    }
                }
            }

            var reportRfreshDate = userSnapshots[0].ReportRefreshDate;

            // Find all records with report refresh date note equal to current and delete
            // Clear out users who have had activity since we last reminded them
            string filter = $"ReportRefreshDate ne '{reportRfreshDate}'";
            var queryResults = _userLastUsageTableClient.QueryAsync<TableEntity>(filter);

            try
            {
                // Query all records with filter
                await foreach (TableEntity entity in queryResults)
                {
                    await _userLastUsageTableClient.DeleteEntityAsync(entity.PartitionKey, entity.RowKey);
                }
            }
            catch (RequestFailedException ex)
            {
                Console.WriteLine($"Error deleting records: {ex.Message}");
            }

            return DAUadded;

        }

        private async Task UpdateReportRefreshDate(string reportRefreshDate, string timeFrame)
        {
            // switch on timeFrame to determine startdate
            timeFrame = timeFrame.ToLowerInvariant();
            var startDate = timeFrame switch
            {
                "daily" => reportRefreshDate,
                "weekly" => GetWeekStartDate(DateTime.ParseExact(reportRefreshDate, "yyyy-MM-dd", null)),
                "monthly" => GetMonthStartDate(DateTime.ParseExact(reportRefreshDate, "yyyy-MM-dd", null)),
                _ => throw new ArgumentException("Invalid timeFrame")
            };

            try
            {
                // Create the daily refresh date
                var tableEntity = new TableEntity("ReportRefreshDate", timeFrame)
                {
                    { "ReportRefreshDate", reportRefreshDate },
                    { "StartDate", startDate }
                };

                // Try to add the entity if it doesn't exist
                await _reportRefreshDateTableClient.AddEntityAsync(tableEntity);

            }
            catch (Azure.RequestFailedException ex) when (ex.Status == 409) // Conflict indicates the entity already exists
            {
                // Merge the entity if it already exists
                var existingTableEntity = await _reportRefreshDateTableClient.GetEntityAsync<TableEntity>("ReportRefreshDate", timeFrame);

                existingTableEntity.Value["ReportRefreshDate"] = reportRefreshDate;
                existingTableEntity.Value["StartDate"] = startDate;

                await _reportRefreshDateTableClient.UpdateEntityAsync(existingTableEntity.Value, ETag.All, TableUpdateMode.Merge);
            }
        }

        public async Task ResetUsersAppStreak(AppType appType, string upn)
        {

            // filter to get users record for app
            // Get the existing entity
            try
            {
                // Get the existing entity
                var existingTableEntity = await _userAllTimeTableClient.GetEntityAsync<TableEntity>(upn, appType.ToString());
                // Perform your logic here with existingTableEntity

                existingTableEntity.Value["CurrentDailyStreak"] = 0;

                await _userAllTimeTableClient.UpdateEntityAsync(existingTableEntity.Value, ETag.All, TableUpdateMode.Merge);

            }
            catch (RequestFailedException ex) when (ex.Status == 404)
            {
                // Entity not found - nothing to reset
            }

        }

        public async Task UpdateUserAllTimeSnapshots(M365CopilotUsage dailySnapshots)
        {
            // Get User Activity
            var userActivity = ConvertToUsageDictionary(dailySnapshots);

            foreach (var (app, dailyUsage) in userActivity)
            {

                try
                {
                    // Get the existing entity
                    var existingTableEntity = await _userAllTimeTableClient.GetEntityAsync<TableEntity>(dailySnapshots.UserPrincipalName, app);

                    // If usage 
                    if (dailyUsage)
                    {
                        // Increment the daily all time activity count
                        existingTableEntity.Value["DailyAllTimeActivityCount"] = (int)existingTableEntity.Value["DailyAllTimeActivityCount"] + 1;
                        existingTableEntity.Value["CurrentDailyStreak"] = (int)existingTableEntity.Value["DailyAllTimeActivityCount"] + 1;
                        existingTableEntity.Value["BestDailyStreak"] = Math.Max((int)existingTableEntity.Value["BestDailyStreak"], (int)existingTableEntity.Value["CurrentDailyStreak"]);

                        await _userAllTimeTableClient.UpdateEntityAsync(existingTableEntity.Value, ETag.All, TableUpdateMode.Merge);
                    }
                    else
                    {
                        // Reset the current daily streak
                        existingTableEntity.Value["CurrentDailyStreak"] = 0;
                        await _userAllTimeTableClient.UpdateEntityAsync(existingTableEntity.Value, ETag.All, TableUpdateMode.Merge);
                    }
                }
                catch (RequestFailedException ex) when (ex.Status == 404)
                {

                    // if usage create first item
                    var newTableEntity = new TableEntity(dailySnapshots.UserPrincipalName, app)
                    {
                        { "DisplayName", dailySnapshots.DisplayName },
                        { "DailyAllTimeActivityCount", dailyUsage ? 1 : 0 },
                        { "CurrentDailyStreak", dailyUsage ? 1 : 0 },
                        { "BestDailyStreak", dailyUsage ? 1 : 0 }
                    };

                    await _userAllTimeTableClient.AddEntityAsync(newTableEntity);
                }
                catch (RequestFailedException ex)
                {
                    Console.WriteLine($"Error updating records: {ex.Message}");
                }
            }
        }

        public async Task UpdateUserMonthlySnapshots(M365CopilotUsage dailySnapshots)
        {
            var firstOfMonthForSnapshot = DateTime.ParseExact(dailySnapshots.ReportRefreshDate, "yyyy-MM-dd", null)
                .AddDays(-1 * DateTime.ParseExact(dailySnapshots.ReportRefreshDate, "yyyy-MM-dd", null).Day + 1)
                .ToString("yyyy-MM-dd");

            var userActivity = ConvertToUserActivity(dailySnapshots);

            try
            {
                // Get the existing entity
                var existingTableEntity = await _userMonthlyTableClient.GetEntityAsync<TableEntity>(firstOfMonthForSnapshot, dailySnapshots.UserPrincipalName);

                // Increment the daily counts
                existingTableEntity.Value["DailyTeamsActivityCount"] = (int)existingTableEntity.Value["DailyTeamsActivityCount"] + (userActivity.DailyTeamsActivity ? 1 : 0);
                existingTableEntity.Value["DailyCopilotChatActivityCount"] = (int)existingTableEntity.Value["DailyCopilotChatActivityCount"] + (userActivity.DailyCopilotChatActivity ? 1 : 0);
                existingTableEntity.Value["DailyOutlookActivityCount"] = (int)existingTableEntity.Value["DailyOutlookActivityCount"] + (userActivity.DailyOutlookActivity ? 1 : 0);
                existingTableEntity.Value["DailyWordActivityCount"] = (int)existingTableEntity.Value["DailyWordActivityCount"] + (userActivity.DailyWordActivity ? 1 : 0);
                existingTableEntity.Value["DailyExcelActivityCount"] = (int)existingTableEntity.Value["DailyExcelActivityCount"] + (userActivity.DailyExcelActivity ? 1 : 0);
                existingTableEntity.Value["DailyPowerPointActivityCount"] = (int)existingTableEntity.Value["DailyPowerPointActivityCount"] + (userActivity.DailyPowerPointActivity ? 1 : 0);
                existingTableEntity.Value["DailyOneNoteActivityCount"] = (int)existingTableEntity.Value["DailyOneNoteActivityCount"] + (userActivity.DailyOneNoteActivity ? 1 : 0);
                existingTableEntity.Value["DailyLoopActivityCount"] = (int)existingTableEntity.Value["DailyLoopActivityCount"] + (userActivity.DailyLoopActivity ? 1 : 0);
                existingTableEntity.Value["DailyAllActivityCount"] = (int)existingTableEntity.Value["DailyAllActivityCount"] + (userActivity.DailyCopilotAllUpActivity ? 1 : 0);

                await _userMonthlyTableClient.UpdateEntityAsync(existingTableEntity.Value, ETag.All, TableUpdateMode.Merge);
            }
            catch (RequestFailedException ex) when (ex.Status == 404)
            {
                // Entity not found - create new entity
                var newTableEntity = new TableEntity(firstOfMonthForSnapshot, dailySnapshots.UserPrincipalName)
                {
                    { "DisplayName", dailySnapshots.DisplayName },
                    { "DailyTeamsActivityCount", userActivity.DailyTeamsActivity ? 1 : 0 },
                    { "DailyCopilotChatActivityCount", userActivity.DailyCopilotChatActivity ? 1 : 0 },
                    { "DailyOutlookActivityCount", userActivity.DailyOutlookActivity ? 1 : 0 },
                    { "DailyWordActivityCount", userActivity.DailyWordActivity ? 1 : 0 },
                    { "DailyExcelActivityCount", userActivity.DailyExcelActivity ? 1 : 0 },
                    { "DailyPowerPointActivityCount", userActivity.DailyPowerPointActivity ? 1 : 0 },
                    { "DailyOneNoteActivityCount", userActivity.DailyOneNoteActivity ? 1 : 0 },
                    { "DailyLoopActivityCount", userActivity.DailyLoopActivity ? 1 : 0 },
                    { "DailyAllActivityCount", userActivity.DailyCopilotAllUpActivity ? 1 : 0 }
                };

                await _userMonthlyTableClient.AddEntityAsync(newTableEntity);
            }
            catch (RequestFailedException ex)
            {
                Console.WriteLine($"Error updating records: {ex.Message}");
            }
        }

        public async Task UpdateUserWeeklySnapshots(M365CopilotUsage dailySnapshots)
        {
            var date = DateTime.ParseExact(dailySnapshots.ReportRefreshDate, "yyyy-MM-dd", null);
            var dayOfWeek = (int)date.DayOfWeek;
            var daysToSubtract = dayOfWeek == 0 ? 6 : dayOfWeek - 1; // Adjust for Sunday
            var firstMondayOfWeeklySnapshot = date.AddDays(-daysToSubtract).ToString("yyyy-MM-dd");

            var userActivity = ConvertToUserActivity(dailySnapshots);

            try
            {
                // Get the existing entity
                var existingTableEntity = await _userWeeklyTableClient.GetEntityAsync<TableEntity>(firstMondayOfWeeklySnapshot, dailySnapshots.UserPrincipalName);

                // Increment the daily counts
                existingTableEntity.Value["DailyTeamsActivityCount"] = (int)existingTableEntity.Value["DailyTeamsActivityCount"] + (userActivity.DailyTeamsActivity ? 1 : 0);
                existingTableEntity.Value["DailyCopilotChatActivityCount"] = (int)existingTableEntity.Value["DailyCopilotChatActivityCount"] + (userActivity.DailyCopilotChatActivity ? 1 : 0);
                existingTableEntity.Value["DailyOutlookActivityCount"] = (int)existingTableEntity.Value["DailyOutlookActivityCount"] + (userActivity.DailyOutlookActivity ? 1 : 0);
                existingTableEntity.Value["DailyWordActivityCount"] = (int)existingTableEntity.Value["DailyWordActivityCount"] + (userActivity.DailyWordActivity ? 1 : 0);
                existingTableEntity.Value["DailyExcelActivityCount"] = (int)existingTableEntity.Value["DailyExcelActivityCount"] + (userActivity.DailyExcelActivity ? 1 : 0);
                existingTableEntity.Value["DailyPowerPointActivityCount"] = (int)existingTableEntity.Value["DailyPowerPointActivityCount"] + (userActivity.DailyPowerPointActivity ? 1 : 0);
                existingTableEntity.Value["DailyOneNoteActivityCount"] = (int)existingTableEntity.Value["DailyOneNoteActivityCount"] + (userActivity.DailyOneNoteActivity ? 1 : 0);
                existingTableEntity.Value["DailyLoopActivityCount"] = (int)existingTableEntity.Value["DailyLoopActivityCount"] + (userActivity.DailyLoopActivity ? 1 : 0);
                existingTableEntity.Value["DailyAllActivityCount"] = (int)existingTableEntity.Value["DailyAllActivityCount"] + (userActivity.DailyCopilotAllUpActivity ? 1 : 0);

                await _userWeeklyTableClient.UpdateEntityAsync(existingTableEntity.Value, ETag.All, TableUpdateMode.Merge);
            }
            catch (RequestFailedException ex) when (ex.Status == 404)
            {
                // Entity not found - create new entity
                var newTableEntity = new TableEntity(firstMondayOfWeeklySnapshot, dailySnapshots.UserPrincipalName)
                {
                    { "DisplayName", dailySnapshots.DisplayName },
                    { "DailyTeamsActivityCount", userActivity.DailyTeamsActivity ? 1 : 0 },
                    { "DailyCopilotChatActivityCount", userActivity.DailyCopilotChatActivity ? 1 : 0 },
                    { "DailyOutlookActivityCount", userActivity.DailyOutlookActivity ? 1 : 0 },
                    { "DailyWordActivityCount", userActivity.DailyWordActivity ? 1 : 0 },
                    { "DailyExcelActivityCount", userActivity.DailyExcelActivity ? 1 : 0 },
                    { "DailyPowerPointActivityCount", userActivity.DailyPowerPointActivity ? 1 : 0 },
                    { "DailyOneNoteActivityCount", userActivity.DailyOneNoteActivity ? 1 : 0 },
                    { "DailyLoopActivityCount", userActivity.DailyLoopActivity ? 1 : 0 },
                    { "DailyAllActivityCount", userActivity.DailyCopilotAllUpActivity ? 1 : 0 }
                };

                await _userWeeklyTableClient.AddEntityAsync(newTableEntity);
            }
            catch (RequestFailedException ex)
            {
                Console.WriteLine($"Error updating records: {ex.Message}");
            }
        }

        private UserActivity ConvertToUserActivity(M365CopilotUsage user)
        {
            // if last activity date is not the same as the report refresh date, everything is false, simly return
            if (user.LastActivityDate != user.ReportRefreshDate)
            {
                return new UserActivity
                {
                    ReportDate = DateTime.SpecifyKind(DateTime.ParseExact(user.ReportRefreshDate, "yyyy-MM-dd", null), DateTimeKind.Utc),
                    UPN = user.UserPrincipalName,
                    DisplayName = user.DisplayName,
                    DailyTeamsActivity = false,
                    DailyOutlookActivity = false,
                    DailyWordActivity = false,
                    DailyExcelActivity = false,
                    DailyPowerPointActivity = false,
                    DailyOneNoteActivity = false,
                    DailyLoopActivity = false,
                    DailyCopilotChatActivity = false
                };
            }

            // CopilotAllUpActivity
            // if any of the values are true, add another entry for CopilotAllUp
            bool copilotAllUpActivity = false;

            // it's a mere string comparison
            bool DailyUsage(string lastActivityDate, string reportRefreshDate)
            {
                if (lastActivityDate == reportRefreshDate)
                {
                    copilotAllUpActivity = true;
                    return true;
                }
                return false;
            }

            return new UserActivity
            {
                ReportDate = DateTime.SpecifyKind(DateTime.ParseExact(user.ReportRefreshDate, "yyyy-MM-dd", null), DateTimeKind.Utc),
                UPN = user.UserPrincipalName,
                DisplayName = user.DisplayName,
                DailyTeamsActivity = DailyUsage(user.MicrosoftTeamsCopilotLastActivityDate, user.ReportRefreshDate),
                DailyOutlookActivity = DailyUsage(user.OutlookCopilotLastActivityDate, user.ReportRefreshDate),
                DailyWordActivity = DailyUsage(user.WordCopilotLastActivityDate, user.ReportRefreshDate),
                DailyExcelActivity = DailyUsage(user.ExcelCopilotLastActivityDate, user.ReportRefreshDate),
                DailyPowerPointActivity = DailyUsage(user.PowerPointCopilotLastActivityDate, user.ReportRefreshDate),
                DailyOneNoteActivity = DailyUsage(user.OneNoteCopilotLastActivityDate, user.ReportRefreshDate),
                DailyLoopActivity = DailyUsage(user.LoopCopilotLastActivityDate, user.ReportRefreshDate),
                DailyCopilotChatActivity = DailyUsage(user.CopilotChatLastActivityDate, user.ReportRefreshDate),
                DailyCopilotAllUpActivity = copilotAllUpActivity
            };
        }

        private Dictionary<string, bool> ConvertToUsageDictionary(M365CopilotUsage user)
        {
            bool DailyUsage(string lastActivityDate, string reportRefreshDate)
            {
                return lastActivityDate == reportRefreshDate;
            }

            var usage = new Dictionary<string, bool>
            {
                { "Teams", DailyUsage(user.MicrosoftTeamsCopilotLastActivityDate, user.ReportRefreshDate) },
                { "Outlook", DailyUsage(user.OutlookCopilotLastActivityDate, user.ReportRefreshDate) },
                { "Word", DailyUsage(user.WordCopilotLastActivityDate, user.ReportRefreshDate) },
                { "Excel", DailyUsage(user.ExcelCopilotLastActivityDate, user.ReportRefreshDate) },
                { "PowerPoint", DailyUsage(user.PowerPointCopilotLastActivityDate, user.ReportRefreshDate) },
                { "OneNote", DailyUsage(user.OneNoteCopilotLastActivityDate, user.ReportRefreshDate) },
                { "Loop", DailyUsage(user.LoopCopilotLastActivityDate, user.ReportRefreshDate) },
                { "CopilotChat", DailyUsage(user.CopilotChatLastActivityDate, user.ReportRefreshDate) }
            };

            // if any of the values are true, add another entry for CopilotAllUp
            if (usage.Values.Any(v => v))
            {
                usage.Add("All", true);
            }
            else
            {
                usage.Add("All", false);
            }

            return usage;
        }

        public async Task<List<string>> GetUsersWhoHaveCompletedActivity(string app, string count, string timeFrame, string date)
        {
            // switch statement to get the correct table on timeFrame
            var tableClient = timeFrame.ToLowerInvariant() switch
            {
                "daily" => _userDAUTableClient,
                "weekly" => _userWeeklyTableClient,
                "monthly" => _userMonthlyTableClient,
                "alltime" => _userAllTimeTableClient,
                _ => throw new ArgumentException("Invalid timeFrame")
            };

            // Define the query filter for weekly, monthly 
            string filter = $"PartitionKey eq '{date}' and Daily{app}Activity ge {count}";

            // Define the query filter for daily
            if (timeFrame == "daily")
            {
                filter = $"PartitionKey eq '{date}' and Daily{app}Activity eq true";
            }

            // Define the query filter for alltime
            if (timeFrame == "alltime")
            {
                filter = $"RowKey eq '{app}' and DailyAllTimeActivityCount ge {count}";
            }

            // log the filter
            _logger.LogInformation($"Filter for {timeFrame}: {filter}");

            // Get the users
            var users = new List<string>();
            try
            {
                // Query all records with filter
                AsyncPageable<TableEntity> queryResults = tableClient.QueryAsync<TableEntity>(filter);

                await foreach (TableEntity entity in queryResults)
                {
                    users.Add(entity.RowKey);
                }
            }
            catch (RequestFailedException ex)
            {
                _logger.LogError($"Error retrieving records: {ex.Message}");
                throw;
            }

            // return some users for testing
            return users;
        }
        public async Task<List<string>> GetUsersWhoHaveCompletedActivity(List<string> apps, string count, string timeFrame, string date)
        {
            // switch statement to get the correct table on timeFrame
            var tableClient = timeFrame.ToLowerInvariant() switch
            {
                "daily" => _userDAUTableClient,
                "weekly" => _userWeeklyTableClient,
                "monthly" => _userMonthlyTableClient,
                "alltime" => _userAllTimeTableClient,
                _ => throw new ArgumentException("Invalid timeFrame")
            };

            // For monthly (and other timeframes except daily and alltime)
            string appsFilterString = string.Join(" and ", apps.Select(app => $"Daily{app}Activity ge {count}"));
            string filter = $"PartitionKey eq '{date}' and {appsFilterString}";

            // Define the query filter for daily
            if (timeFrame == "daily")
            {
                // replace all ge {count} with eq true
                appsFilterString = appsFilterString.Replace($"ge {count}", $"eq true");

                filter = $"PartitionKey eq '{date}' and {appsFilterString}";
            }

            // Define the query filter for alltime
            if (timeFrame == "alltime")
            {
                // All time is different as we need a row for each app
                appsFilterString = string.Join(" or ", apps.Select(app => $"RowKey eq '{app}' and DailyAllTimeActivityCount ge {count}"));
                filter = appsFilterString;
            }

            // log the filter
            _logger.LogInformation($"Filter for {timeFrame}: {filter}");

            // Get the users
            var users = new List<string>();
            try
            {
                // Query all records with filter
                AsyncPageable<TableEntity> queryResults = tableClient.QueryAsync<TableEntity>(filter);

                await foreach (TableEntity entity in queryResults)
                {
                    users.Add(entity.RowKey);
                }
            }
            catch (RequestFailedException ex)
            {
                Console.WriteLine($"Error retrieving records: {ex.Message}");
            }

            // return some users for testing
            return users;
        }
        static string GetWeekStartDate(DateTime date)
        {
            // Get the Monday of the current week
            var dayOfWeek = (int)date.DayOfWeek;
            var daysToSubtract = dayOfWeek == 0 ? 6 : dayOfWeek - 1; // Adjust for Sunday
            return date.AddDays(-daysToSubtract).ToString("yyyy-MM-dd");
        }

        static string GetMonthStartDate(DateTime date)
        {
            // Get the first day of the current month
            return date
                .AddDays(-1 * date.Day + 1)
                .ToString("yyyy-MM-dd");
        }

        public async Task<string?> GetStartDate(string timeFrame)
        {
            // Get the report refresh date for the timeFrame
            try
            {
                var existingTableEntity = await _reportRefreshDateTableClient.GetEntityAsync<TableEntity>("ReportRefreshDate", timeFrame);
                return existingTableEntity.Value["StartDate"].ToString();
            }
            catch (RequestFailedException ex) when (ex.Status == 404)
            {
                // Entity not found - nothing to do
                return null;
            }
        }

        public async Task SeedDailyActivitiesAsync(List<UserActivity> userActivitiesSeed)
        {
            // Get daily table
            foreach (var userEntity in userActivitiesSeed)
            {
                // Add user record
                var tableEntity = new TableEntity(userEntity.ReportDate.ToString("yyyy-MM-dd"), userEntity.UPN)
                {
                    { "ReportDate", userEntity.ReportDate },
                    { "DisplayName", userEntity.DisplayName },
                    { "DailyTeamsActivity", userEntity.DailyTeamsActivity },
                    { "DailyOutlookActivity", userEntity.DailyOutlookActivity },
                    { "DailyWordActivity", userEntity.DailyWordActivity },
                    { "DailyExcelActivity", userEntity.DailyExcelActivity },
                    { "DailyPowerPointActivity", userEntity.DailyPowerPointActivity },
                    { "DailyOneNoteActivity", userEntity.DailyOneNoteActivity },
                    { "DailyLoopActivity", userEntity.DailyLoopActivity },
                    { "DailyCopilotChatActivity", userEntity.DailyCopilotChatActivity },
                    { "DailyAllActivity", userEntity.DailyCopilotAllUpActivity }
                };

                try
                {
                    // Try to add the entity if it doesn't exist
                    await _userDAUTableClient.AddEntityAsync(tableEntity);
                    _logger.LogInformation($"Added daily seed entity for {userEntity.UPN}");
                }
                catch (Azure.RequestFailedException ex) when (ex.Status == 409) // Conflict indicates the entity already exists
                {
                    // Merge the entity if it already exists
                    await _userDAUTableClient.UpdateEntityAsync(tableEntity, ETag.All, TableUpdateMode.Merge);
                }
            }

        }

        public async Task SeedWeeklyActivitiesAsync(List<WeeklyUsage> userActivitiesSeed)
        {
            foreach (var userEntity in userActivitiesSeed)
            {
                // Add user record
                var tableEntity = new TableEntity(userEntity.StartDate.ToString("yyyy-MM-dd"), userEntity.UPN)
                {
                    { "StartDate", userEntity.StartDate },
                    { "DisplayName", userEntity.DisplayName },
                    { "DailyTeamsActivityCount", userEntity.DailyTeamsActivityCount },
                    { "DailyOutlookActivityCount", userEntity.DailyOutlookActivityCount },
                    { "DailyWordActivityCount", userEntity.DailyWordActivityCount },
                    { "DailyExcelActivityCount", userEntity.DailyExcelActivityCount },
                    { "DailyPowerPointActivityCount", userEntity.DailyPowerPointActivityCount },
                    { "DailyOneNoteActivityCount", userEntity.DailyOneNoteActivityCount },
                    { "DailyLoopActivityCount", userEntity.DailyLoopActivityCount },
                    { "DailyCopilotChatActivityCount", userEntity.DailyCopilotChatActivityCount },
                    { "DailyAllActivityCount", userEntity.DailyAllActivityCount }
                };

                try
                {
                    // Try to add the entity if it doesn't exist
                    await _userWeeklyTableClient.AddEntityAsync(tableEntity);
                    _logger.LogInformation($"Added weekly seed entity for {userEntity.UPN}");
                }
                catch (Azure.RequestFailedException ex) when (ex.Status == 409) // Conflict indicates the entity already exists
                {
                    // Merge the entity if it already exists
                    await _userWeeklyTableClient.UpdateEntityAsync(tableEntity, ETag.All, TableUpdateMode.Merge);
                }
            }
        }

        public async Task SeedMonthlyActivitiesAsync(List<MonthlyUsage> userActivitiesSeed)
        {
            // Get daily table
            foreach (var userEntity in userActivitiesSeed)
            {
                // Add user record
                var tableEntity = new TableEntity(userEntity.StartDate.ToString("yyyy-MM-dd"), userEntity.UPN)
                {
                    { "StartDate", userEntity.StartDate },
                    { "DisplayName", userEntity.DisplayName },
                    { "DailyTeamsActivityCount", userEntity.DailyTeamsActivityCount },
                    { "DailyOutlookActivityCount", userEntity.DailyOutlookActivityCount },
                    { "DailyWordActivityCount", userEntity.DailyWordActivityCount },
                    { "DailyExcelActivityCount", userEntity.DailyExcelActivityCount },
                    { "DailyPowerPointActivityCount", userEntity.DailyPowerPointActivityCount },
                    { "DailyOneNoteActivityCount", userEntity.DailyOneNoteActivityCount },
                    { "DailyLoopActivityCount", userEntity.DailyLoopActivityCount },
                    { "DailyCopilotChatActivityCount", userEntity.DailyCopilotChatActivityCount },
                    { "DailyAllActivityCount", userEntity.DailyAllActivityCount }
                };

                try
                {
                    // Try to add the entity if it doesn't exist
                    await _userMonthlyTableClient.AddEntityAsync(tableEntity);
                    _logger.LogInformation($"Added monthly seed entity for {userEntity.UPN}");
                }
                catch (Azure.RequestFailedException ex) when (ex.Status == 409) // Conflict indicates the entity already exists
                {
                    // Merge the entity if it already exists
                    await _userMonthlyTableClient.UpdateEntityAsync(tableEntity, ETag.All, TableUpdateMode.Merge);
                }
            }
        }
    }
}
