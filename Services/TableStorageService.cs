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

        Task UpdateUsersTimeFrameSnapshots(UserActivity dailySnapshots, string timeFrame);
        Task UpdateUserAllTimeSnapshots(UserActivity dailySnapshots);

        Task ResetUsersAppStreak(AppType appType, string upn);

        Task<string?> GetStartDate(string timeFrame);

        Task<List<string>> GetUsersWhoHaveCompletedActivity(List<string> apps, string count, string timeFrame, string startDate);
        Task<List<string>> GetUsersWhoHaveCompletedActivity(string app, string count, string timeFrame, string startDate);

        //Task<List<LeaderboardRow>> GetLeaderboard(string app, bool streak, int count);

        Task<List<InactiveUser>> GetInactiveUsers(int days);
        Task<List<string>> GetUsersWithStreak(List<string> apps, int count);

        // For Seeding
        Task SeedDailyActivitiesAsync(List<UserActivity> userActivitiesSeed);
        Task SeedWeeklyActivitiesAsync(List<WeeklyUsage> userActivitiesSeed);
        Task SeedMonthlyActivitiesAsync(List<MonthlyUsage> userActivitiesSeed);
        Task SeedAllTimeActivityAsync(List<AllTimeUsage> userActivitiesSeed);
        Task SeedInactiveUsersAsync(List<InactiveUser> inactiveUsersSeed);

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
        private readonly TableClient _copilotInteractionDailyAggregationTableClient;
        private readonly ILogger<CopilotUsageSnapshotService> _logger;
        private readonly bool CDXTenant = System.Environment.GetEnvironmentVariable("CDXTenant") == "true";
        private readonly string _userDAUTableName = "CopilotUsageDailySnapshots";
        private readonly string _userLastUsageTableName = "UsersLastUsageTracker";
        private readonly string _userWeeklyTableName = "CopilotUsageWeeklySnapshots";
        private readonly string _userMonthlyTableName = "CopilotUsageMonthlySnapshots";
        private readonly string _userAllTimeTableName = "CopilotUsageAllTimeRecord";
        private readonly string _reportRefreshDateTableName = "ReportRefreshRecord";
        private readonly string _copilotInteractionDailyAggregationTableName = "CopilotInteractionDailyAggregationByUser";

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

            _copilotInteractionDailyAggregationTableClient = _serviceClient.GetTableClient(_copilotInteractionDailyAggregationTableName);
            _copilotInteractionDailyAggregationTableClient.CreateIfNotExists();
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

                // if getting audit data we should get the aggregation data for the user
                // Get the aggregation entity
                var aggregationEntity = await GetDailyAuditDataForUser(userSnap.UserPrincipalName, userSnap.ReportRefreshDate);

                var userEntity = ConvertToUserActivity(userSnap, aggregationEntity);

                try
                {
                    // Try to add the entity if it doesn't exist
                    await _userDAUTableClient.AddEntityAsync(userEntity.ToTableEntity());
                    DAUadded++;

                }
                catch (Azure.RequestFailedException ex) when (ex.Status == 409) // Conflict indicates the entity already exists
                {
                    // Merge the entity if it already exists
                    await _userDAUTableClient.UpdateEntityAsync(userEntity.ToTableEntity(), ETag.All, TableUpdateMode.Merge);
                }


                // We need to update the last weekly, monthly and alltime tables
                await UpdateUserAllTimeSnapshots(userEntity);

                // Update Monthly
                var firstOfMonthForSnapshot = userEntity.ReportDate
                                .AddDays(-1 * userEntity.ReportDate.Day + 1)
                                .ToString("yyyy-MM-dd");

                await UpdateUsersTimeFrameSnapshots(userEntity, firstOfMonthForSnapshot);

                // Update Weekly - Get our timeFrame
                var date = userEntity.ReportDate;
                var dayOfWeek = (int)userEntity.ReportDate.DayOfWeek;
                var daysToSubtract = dayOfWeek == 0 ? 6 : dayOfWeek - 1; // Adjust for Sunday
                var firstMondayOfWeeklySnapshot = date.AddDays(-daysToSubtract).ToString("yyyy-MM-dd");

                await UpdateUsersTimeFrameSnapshots(userEntity, firstMondayOfWeeklySnapshot);


            }

            // Update the timeFrame table
            await UpdateReportRefreshDate(reportRefreshDateString, "daily");
            await UpdateReportRefreshDate(reportRefreshDateString, "weekly");
            await UpdateReportRefreshDate(reportRefreshDateString, "monthly");

            // For notifications - now handled elsewhere
            //return DAUadded;

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
                        // { "LastNotificationDate", today },
                        // { "DaysSinceLastNotification", (double)999 },
                        // { "NotificationCount", 0 },
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

                        // // persit the existing value for LastNotificationDate and NotificationCount
                        // tableEntity["LastNotificationDate"] = existingTableEntity.Value["LastNotificationDate"];

                        // if (existingTableEntity.Value.GetInt32("NotificationCount") != 0)
                        // {
                        //     tableEntity["NotificationCount"] = existingTableEntity.Value["NotificationCount"];
                        // }
                        // else
                        // {
                        //     tableEntity["NotificationCount"] = 1;
                        // }

                        // // Need to up the days since last notification
                        // tableEntity["DaysSinceLastNotification"] = (DateTime.ParseExact(today, "yyyy-MM-dd", null) - DateTime.ParseExact(existingTableEntity.Value["LastNotificationDate"].ToString(), "yyyy-MM-dd", null)).TotalDays;

                        // // check if we need to send a reminder
                        // var daysSinceLastNotification = tableEntity.GetDouble("DaysSinceLastNotification") ?? 0;

                        // if (daysSinceLastNotification >= reminderInterval)
                        // {
                        //     // Add to the notification count
                        //     tableEntity["NotificationCount"] = (existingTableEntity.Value.GetInt32("NotificationCount") ?? 0) + 1;

                        //     // Add to the last notification date
                        //     tableEntity["LastNotificationDate"] = today;
                        // }

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

        private async Task<Response<TableEntity>> GetDailyAuditDataForUser(string uPN, string reportDate)
        {
            try
            {
                // Get the aggregation entity
                var existingTableEntity = await _copilotInteractionDailyAggregationTableClient.GetEntityAsync<TableEntity>(reportDate, uPN);
                return existingTableEntity;
            }
            catch (RequestFailedException ex)
            {
                _logger.LogError($"Failed to retrieve entity for UPN: {uPN}, ReportDate: {reportDate}. Error: {ex.Message}");
                throw; // Re-throw the exception or return a default value if needed
            }
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
                var existingTableEntity = await _userAllTimeTableClient.GetEntityAsync<AllTimeUsage>(upn, appType.ToString());
                // Perform your logic here with existingTableEntity

                existingTableEntity.Value.CurrentDailyStreak = 0;

                await _userAllTimeTableClient.UpdateEntityAsync(existingTableEntity.Value, ETag.All, TableUpdateMode.Merge);

            }
            catch (RequestFailedException ex) when (ex.Status == 404)
            {
                // Entity not found - nothing to reset
            }

        }

        public async Task UpdateUserAllTimeSnapshots(UserActivity userActivity)
        {
            // Get User Activity
            var userActivityDictionary = ConvertToUsageDictionary(userActivity);

            foreach (var (app, tuple) in userActivityDictionary)
            {
                bool dailyUsage = tuple.Item1;
                int interactionCount = tuple.Item2;

                try
                {
                    // Get the existing entity
                    var existingTableEntity = await _userAllTimeTableClient.GetEntityAsync<AllTimeUsage>(userActivity.UPN, app.ToString());

                    // If usage 
                    if (dailyUsage)
                    {
                        // Increment the daily all time activity count
                        existingTableEntity.Value.DailyAllTimeActivityCount = existingTableEntity.Value.DailyAllTimeActivityCount + 1;
                        existingTableEntity.Value.CurrentDailyStreak = existingTableEntity.Value.CurrentDailyStreak + 1;
                        existingTableEntity.Value.BestDailyStreak = Math.Max(existingTableEntity.Value.BestDailyStreak, existingTableEntity.Value.CurrentDailyStreak);
                        existingTableEntity.Value.AllTimeInteractionCount = existingTableEntity.Value.AllTimeInteractionCount + interactionCount;

                        await _userAllTimeTableClient.UpdateEntityAsync(existingTableEntity.Value, ETag.All, TableUpdateMode.Merge);
                    }
                    else
                    {
                        // Reset the current daily streak
                        existingTableEntity.Value.CurrentDailyStreak = 0;
                        await _userAllTimeTableClient.UpdateEntityAsync(existingTableEntity.Value, ETag.All, TableUpdateMode.Merge);
                    }
                }
                catch (RequestFailedException ex) when (ex.Status == 404)
                {

                    // if usage create first item
                    var newAllTimeUsage = new AllTimeUsage()
                    {
                        UPN = userActivity.UPN,
                        App = app,
                        DailyAllTimeActivityCount = dailyUsage ? 1 : 0,
                        AllTimeInteractionCount = interactionCount,
                        CurrentDailyStreak = dailyUsage ? 1 : 0,
                        BestDailyStreak = dailyUsage ? 1 : 0,
                    };

                    await _userAllTimeTableClient.AddEntityAsync(newAllTimeUsage.ToTableEntity());
                }
                catch (RequestFailedException ex)
                {
                    Console.WriteLine($"Error updating records: {ex.Message}");
                }
            }
        }

        public async Task UpdateUsersTimeFrameSnapshots(UserActivity userActivity, string timeFrame)
        {
            
            try
            {
                // Get the existing entity
                var existingTableEntity = await _userMonthlyTableClient.GetEntityAsync<TimeFrameUsage>(timeFrame, userActivity.UPN);

                // Increment the daily counts
                existingTableEntity.Value.DailyTeamsActivityCount += userActivity.DailyTeamsActivity ? 1 : 0;
                existingTableEntity.Value.DailyCopilotChatActivityCount += userActivity.DailyCopilotChatActivity ? 1 : 0;
                existingTableEntity.Value.DailyOutlookActivityCount += userActivity.DailyOutlookActivity ? 1 : 0;
                existingTableEntity.Value.DailyWordActivityCount += userActivity.DailyWordActivity ? 1 : 0;
                existingTableEntity.Value.DailyExcelActivityCount += userActivity.DailyExcelActivity ? 1 : 0;
                existingTableEntity.Value.DailyPowerPointActivityCount += userActivity.DailyPowerPointActivity ? 1 : 0;
                existingTableEntity.Value.DailyOneNoteActivityCount += userActivity.DailyOneNoteActivity ? 1 : 0;
                existingTableEntity.Value.DailyLoopActivityCount += userActivity.DailyLoopActivity ? 1 : 0;
                existingTableEntity.Value.DailyAllActivityCount += userActivity.DailyCopilotAllUpActivity ? 1 : 0;

                // Increment additional activity counts
                existingTableEntity.Value.DailyMACActivityCount += userActivity.DailyMACActivity ? 1 : 0;
                existingTableEntity.Value.DailyDesignerActivityCount += userActivity.DailyDesignerActivity ? 1 : 0;
                existingTableEntity.Value.DailySharePointActivityCount += userActivity.DailySharePointActivity ? 1 : 0;
                existingTableEntity.Value.DailyPlannerActivityCount += userActivity.DailyPlannerActivity ? 1 : 0;
                existingTableEntity.Value.DailyWhiteboardActivityCount += userActivity.DailyWhiteboardActivity ? 1 : 0;
                existingTableEntity.Value.DailyStreamActivityCount += userActivity.DailyStreamActivity ? 1 : 0;
                existingTableEntity.Value.DailyFormsActivityCount += userActivity.DailyFormsActivity ? 1 : 0;
                existingTableEntity.Value.DailyCopilotActionActivityCount += userActivity.DailyCopilotActionActivity ? 1 : 0;
                existingTableEntity.Value.DailyWebPluginActivityCount += userActivity.DailyWebPluginActivity ? 1 : 0;

                // Increment interaction counts
                existingTableEntity.Value.TeamsInteractionCount += userActivity.DailyTeamsInteractionCount;
                existingTableEntity.Value.CopilotChatInteractionCount += userActivity.DailyCopilotChatInteractionCount;
                existingTableEntity.Value.OutlookInteractionCount += userActivity.DailyOutlookInteractionCount;
                existingTableEntity.Value.WordInteractionCount += userActivity.DailyWordInteractionCount;
                existingTableEntity.Value.ExcelInteractionCount += userActivity.DailyExcelInteractionCount;
                existingTableEntity.Value.PowerPointInteractionCount += userActivity.DailyPowerPointInteractionCount;
                existingTableEntity.Value.OneNoteInteractionCount += userActivity.DailyOneNoteInteractionCount;
                existingTableEntity.Value.LoopInteractionCount += userActivity.DailyLoopInteractionCount;
                existingTableEntity.Value.MACInteractionCount += userActivity.DailyMACInteractionCount;
                existingTableEntity.Value.DesignerInteractionCount += userActivity.DailyDesignerInteractionCount;
                existingTableEntity.Value.SharePointInteractionCount += userActivity.DailySharePointInteractionCount;
                existingTableEntity.Value.PlannerInteractionCount += userActivity.DailyPlannerInteractionCount;
                existingTableEntity.Value.WhiteboardInteractionCount += userActivity.DailyWhiteboardInteractionCount;
                existingTableEntity.Value.StreamInteractionCount += userActivity.DailyStreamInteractionCount;
                existingTableEntity.Value.FormsInteractionCount += userActivity.DailyFormsInteractionCount;
                existingTableEntity.Value.CopilotActionInteractionCount += userActivity.DailyCopilotActionCount;
                existingTableEntity.Value.WebPluginInteractionCount += userActivity.DailyWebPluginInteractions;
                existingTableEntity.Value.AllInteractionCount += userActivity.DailyAllInteractionCount;

                // Update the entity in the table
                await _userMonthlyTableClient.UpdateEntityAsync(existingTableEntity.Value, ETag.All, TableUpdateMode.Merge);

            }
            catch (RequestFailedException ex) when (ex.Status == 404)
            {
                // Entity not found - create new entity
                var newEntity = new TimeFrameUsage
                {
                    StartDate = DateTime.Parse(timeFrame),
                    UPN = userActivity.UPN,
                    DailyTeamsActivityCount = userActivity.DailyTeamsActivity ? 1 : 0,
                    DailyCopilotChatActivityCount = userActivity.DailyCopilotChatActivity ? 1 : 0,
                    DailyOutlookActivityCount = userActivity.DailyOutlookActivity ? 1 : 0,
                    DailyWordActivityCount = userActivity.DailyWordActivity ? 1 : 0,
                    DailyExcelActivityCount = userActivity.DailyExcelActivity ? 1 : 0,
                    DailyPowerPointActivityCount = userActivity.DailyPowerPointActivity ? 1 : 0,
                    DailyOneNoteActivityCount = userActivity.DailyOneNoteActivity ? 1 : 0,
                    DailyLoopActivityCount = userActivity.DailyLoopActivity ? 1 : 0,
                    DailyAllActivityCount = userActivity.DailyCopilotAllUpActivity ? 1 : 0,
                    DailyMACActivityCount = userActivity.DailyMACActivity ? 1 : 0,
                    DailyDesignerActivityCount = userActivity.DailyDesignerActivity ? 1 : 0,
                    DailySharePointActivityCount = userActivity.DailySharePointActivity ? 1 : 0,
                    DailyPlannerActivityCount = userActivity.DailyPlannerActivity ? 1 : 0,
                    DailyWhiteboardActivityCount = userActivity.DailyWhiteboardActivity ? 1 : 0,
                    DailyStreamActivityCount = userActivity.DailyStreamActivity ? 1 : 0,
                    DailyFormsActivityCount = userActivity.DailyFormsActivity ? 1 : 0,
                    DailyCopilotActionActivityCount = userActivity.DailyCopilotActionActivity ? 1 : 0,
                    DailyWebPluginActivityCount = userActivity.DailyWebPluginActivity ? 1 : 0,
                    TeamsInteractionCount = userActivity.DailyTeamsInteractionCount,
                    CopilotChatInteractionCount = userActivity.DailyCopilotChatInteractionCount,
                    OutlookInteractionCount = userActivity.DailyOutlookInteractionCount,
                    WordInteractionCount = userActivity.DailyWordInteractionCount,
                    ExcelInteractionCount = userActivity.DailyExcelInteractionCount,
                    PowerPointInteractionCount = userActivity.DailyPowerPointInteractionCount,
                    OneNoteInteractionCount = userActivity.DailyOneNoteInteractionCount,
                    LoopInteractionCount = userActivity.DailyLoopInteractionCount,
                    MACInteractionCount = userActivity.DailyMACInteractionCount,
                    DesignerInteractionCount = userActivity.DailyDesignerInteractionCount,
                    SharePointInteractionCount = userActivity.DailySharePointInteractionCount,
                    PlannerInteractionCount = userActivity.DailyPlannerInteractionCount,
                    WhiteboardInteractionCount = userActivity.DailyWhiteboardInteractionCount,
                    StreamInteractionCount = userActivity.DailyStreamInteractionCount,
                    FormsInteractionCount = userActivity.DailyFormsInteractionCount,
                    CopilotActionInteractionCount = userActivity.DailyCopilotActionCount,
                    WebPluginInteractionCount = userActivity.DailyWebPluginInteractions,
                    AllInteractionCount = userActivity.DailyAllInteractionCount
                };

                await _userMonthlyTableClient.AddEntityAsync(newEntity.ToTableEntity());
            }
            catch (RequestFailedException ex)
            {
                Console.WriteLine($"Error updating records: {ex.Message}");
            }
        }

        private UserActivity ConvertToUserActivity(M365CopilotUsage user, Response<TableEntity> aggregationEntity)
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
                    DailyCopilotChatActivity = false,
                    DailyCopilotAllUpActivity = false,
                    DailyTeamsInteractionCount = 0,
                    DailyOutlookInteractionCount = 0,
                    DailyWordInteractionCount = 0,
                    DailyExcelInteractionCount = 0,
                    DailyPowerPointInteractionCount = 0,
                    DailyOneNoteInteractionCount = 0,
                    DailyLoopInteractionCount = 0,
                    DailyCopilotChatInteractionCount = 0,
                    DailyAllInteractionCount = 0,
                    DailyMACActivity = false,
                    DailyMACInteractionCount = 0,
                    DailyDesignerActivity = false,
                    DailyDesignerInteractionCount = 0,
                    DailySharePointActivity = false,
                    DailySharePointInteractionCount = 0,
                    DailyPlannerActivity = false,
                    DailyPlannerInteractionCount = 0,
                    DailyWhiteboardActivity = false,
                    DailyWhiteboardInteractionCount = 0,
                    DailyStreamActivity = false,
                    DailyStreamInteractionCount = 0,
                    DailyFormsActivity = false,
                    DailyFormsInteractionCount = 0,
                    DailyCopilotActionActivity = false,
                    DailyCopilotActionCount = 0,
                    DailyWebPluginActivity = false,
                    DailyWebPluginInteractions = 0
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

            bool DailyUsageFromCount(int count)
            {
                if (count > 0)
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
                DailyTeamsInteractionCount = aggregationEntity.Value.GetInt32("TeamsInteractions").GetValueOrDefault(),
                DailyOutlookActivity = DailyUsage(user.OutlookCopilotLastActivityDate, user.ReportRefreshDate),
                DailyOutlookInteractionCount = aggregationEntity.Value.GetInt32("OutlookInteractions").GetValueOrDefault(),
                DailyWordActivity = DailyUsage(user.WordCopilotLastActivityDate, user.ReportRefreshDate),
                DailyWordInteractionCount = aggregationEntity.Value.GetInt32("WordInteractions").GetValueOrDefault(),
                DailyExcelActivity = DailyUsage(user.ExcelCopilotLastActivityDate, user.ReportRefreshDate),
                DailyExcelInteractionCount = aggregationEntity.Value.GetInt32("ExcelInteractions").GetValueOrDefault(),
                DailyPowerPointActivity = DailyUsage(user.PowerPointCopilotLastActivityDate, user.ReportRefreshDate),
                DailyPowerPointInteractionCount = aggregationEntity.Value.GetInt32("PowerPointInteractions").GetValueOrDefault(),
                DailyOneNoteActivity = DailyUsage(user.OneNoteCopilotLastActivityDate, user.ReportRefreshDate),
                DailyOneNoteInteractionCount = aggregationEntity.Value.GetInt32("OneNoteInteractions").GetValueOrDefault(),
                DailyLoopActivity = DailyUsage(user.LoopCopilotLastActivityDate, user.ReportRefreshDate),
                DailyLoopInteractionCount = aggregationEntity.Value.GetInt32("LoopInteractions").GetValueOrDefault(),
                DailyCopilotChatActivity = DailyUsage(user.CopilotChatLastActivityDate, user.ReportRefreshDate),
                DailyCopilotChatInteractionCount = aggregationEntity.Value.GetInt32("CopilotChat").GetValueOrDefault(),
                DailyAllInteractionCount = aggregationEntity.Value.GetInt32("TotalCount").GetValueOrDefault(),
                DailyMACActivity = DailyUsageFromCount(aggregationEntity.Value.GetInt32("AdminCenterInteractions").GetValueOrDefault()),
                DailyMACInteractionCount = aggregationEntity.Value.GetInt32("AdminCenterInteractions").GetValueOrDefault(),
                DailyDesignerActivity = DailyUsageFromCount(aggregationEntity.Value.GetInt32("DesignerInteractions").GetValueOrDefault()),
                DailyDesignerInteractionCount = aggregationEntity.Value.GetInt32("DesignerInteractions").GetValueOrDefault(),
                DailySharePointActivity = DailyUsageFromCount(aggregationEntity.Value.GetInt32("SharePointInteractions").GetValueOrDefault()),
                DailySharePointInteractionCount = aggregationEntity.Value.GetInt32("SharePointInteractions").GetValueOrDefault(),
                DailyPlannerActivity = DailyUsageFromCount(aggregationEntity.Value.GetInt32("PlannerInteractions").GetValueOrDefault()),
                DailyPlannerInteractionCount = aggregationEntity.Value.GetInt32("PlannerInteractions").GetValueOrDefault(),
                DailyWhiteboardActivity = DailyUsageFromCount(aggregationEntity.Value.GetInt32("WhiteboardInteractions").GetValueOrDefault()),
                DailyWhiteboardInteractionCount = aggregationEntity.Value.GetInt32("WhiteboardInteractions").GetValueOrDefault(),
                DailyStreamActivity = DailyUsageFromCount(aggregationEntity.Value.GetInt32("StreamInteractions").GetValueOrDefault()),
                DailyStreamInteractionCount = aggregationEntity.Value.GetInt32("StreamInteractions").GetValueOrDefault(),
                DailyFormsActivity = DailyUsageFromCount(aggregationEntity.Value.GetInt32("FormsInteractions").GetValueOrDefault()),
                DailyFormsInteractionCount = aggregationEntity.Value.GetInt32("FormsInteractions").GetValueOrDefault(),
                DailyCopilotActionActivity = DailyUsageFromCount(aggregationEntity.Value.GetInt32("CopilotAction").GetValueOrDefault()),
                DailyCopilotActionCount = aggregationEntity.Value.GetInt32("CopilotAction").GetValueOrDefault(),
                DailyWebPluginActivity = DailyUsageFromCount(aggregationEntity.Value.GetInt32("WebPluginInteractions").GetValueOrDefault()),
                DailyWebPluginInteractions = aggregationEntity.Value.GetInt32("WebPluginInteractions").GetValueOrDefault(),

                DailyCopilotAllUpActivity = copilotAllUpActivity
            };
        }

        private Dictionary<AppType, Tuple<bool, int>> ConvertToUsageDictionary(UserActivity userActivity)
        {

            var usage = new Dictionary<AppType, Tuple<bool, int>>
            {
                { AppType.Teams, new Tuple<bool,int>(userActivity.DailyTeamsActivity, userActivity.DailyTeamsInteractionCount) },
                { AppType.Outlook, new Tuple<bool,int>(userActivity.DailyOutlookActivity, userActivity.DailyOutlookInteractionCount) },
                { AppType.Word, new Tuple<bool,int>(userActivity.DailyWordActivity, userActivity.DailyWordInteractionCount) },
                { AppType.Excel, new Tuple<bool,int>(userActivity.DailyExcelActivity, userActivity.DailyExcelInteractionCount) },
                { AppType.PowerPoint, new Tuple<bool,int>(userActivity.DailyPowerPointActivity, userActivity.DailyPowerPointInteractionCount) },
                { AppType.OneNote, new Tuple<bool,int>(userActivity.DailyOneNoteActivity, userActivity.DailyOneNoteInteractionCount) },
                { AppType.Loop, new Tuple<bool,int>(userActivity.DailyLoopActivity, userActivity.DailyLoopInteractionCount) },
                { AppType.CopilotChat, new Tuple<bool,int>(userActivity.DailyCopilotChatActivity, userActivity.DailyCopilotChatInteractionCount) },
                { AppType.All, new Tuple<bool,int>(userActivity.DailyCopilotAllUpActivity, userActivity.DailyAllInteractionCount) },
                { AppType.MAC, new Tuple<bool,int>(userActivity.DailyMACActivity, userActivity.DailyMACInteractionCount) },
                { AppType.Designer, new Tuple<bool,int>(userActivity.DailyDesignerActivity, userActivity.DailyDesignerInteractionCount) },
                { AppType.SharePoint, new Tuple<bool,int>(userActivity.DailySharePointActivity, userActivity.DailySharePointInteractionCount) },
                { AppType.Planner, new Tuple<bool,int>(userActivity.DailyPlannerActivity, userActivity.DailyPlannerInteractionCount) },
                { AppType.Whiteboard, new Tuple<bool,int>(userActivity.DailyWhiteboardActivity, userActivity.DailyWhiteboardInteractionCount) },
                { AppType.Stream, new Tuple<bool,int>(userActivity.DailyStreamActivity, userActivity.DailyStreamInteractionCount) },
                { AppType.Forms, new Tuple<bool,int>(userActivity.DailyFormsActivity, userActivity.DailyFormsInteractionCount) },
                { AppType.CopilotAction, new Tuple<bool,int>(userActivity.DailyCopilotActionActivity, userActivity.DailyCopilotActionCount) },
                { AppType.WebPlugin, new Tuple<bool,int>(userActivity.DailyWebPluginActivity, userActivity.DailyWebPluginInteractions) }
            };

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
            string appsFilterString = string.Join(" and ", apps.Select(app => $"Daily{app}ActivityCount ge {count}"));
            string filter = $"PartitionKey eq '{date}' and {appsFilterString}";

            // Define the query filter for daily
            if (timeFrame == "daily")
            {
                // replace all ge {count} with eq true
                appsFilterString = appsFilterString.Replace($"Count ge {count}", $" eq true");

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
                    // if all time, we need to add the partition key as well
                    if (timeFrame == "alltime")
                    {
                        users.Add(entity.PartitionKey);
                        continue;
                    }

                    users.Add(entity.RowKey);
                }
            }
            catch (RequestFailedException ex)
            {
                Console.WriteLine($"Error retrieving records: {ex.Message}");
            }

            // if all time
            if (timeFrame == "alltime")
            {
                // check we have a row for each app for each uses, group the list and check count is equal to apps count
                var groupedUsers = users.GroupBy(u => u).Select(g => new { UPN = g.Key, Count = g.Count() }).ToList();
                // filter the users to only those with a streak for all apps
                users = groupedUsers.Where(g => g.Count == apps.Count).Select(g => g.UPN).ToList();
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
                    { "DailyAllActivity", userEntity.DailyCopilotAllUpActivity },

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

        public async Task SeedAllTimeActivityAsync(List<AllTimeUsage> userActivitiesSeed)
        {
            // Get daily table
            foreach (var userEntity in userActivitiesSeed)
            {
                // Add user record
                var tableEntity = new TableEntity(userEntity.UPN, userEntity.App.ToString())
                {
                    { "DisplayName", userEntity.DisplayName },
                    { "DailyAllTimeActivityCount", userEntity.DailyAllTimeActivityCount },
                    { "CurrentDailyStreak", userEntity.CurrentDailyStreak },
                    { "BestDailyStreak", userEntity.BestDailyStreak }
                };

                try
                {
                    // Try to add the entity if it doesn't exist
                    await _userAllTimeTableClient.AddEntityAsync(tableEntity);
                    _logger.LogInformation($"Added alltime seed entity for {userEntity.UPN}");
                }
                catch (Azure.RequestFailedException ex) when (ex.Status == 409) // Conflict indicates the entity already exists
                {
                    // Merge the entity if it already exists
                    await _userAllTimeTableClient.UpdateEntityAsync(tableEntity, ETag.All, TableUpdateMode.Merge);
                }
            }
        }

        public async Task SeedInactiveUsersAsync(List<InactiveUser> userActivitiesSeed)
        {
            // Get daily table
            foreach (var userEntity in userActivitiesSeed)
            {
                // Add user record
                var tableEntity = new TableEntity(userEntity.UPN, userEntity.LastActivityDate.ToString("yyyy-MM-dd"))
                {
                    { "DaysSinceLastActivity", userEntity.DaysSinceLastActivity },
                    { "LastActivityDate", userEntity.LastActivityDate },
                    { "DisplayName", userEntity.DisplayName }
                };

                try
                {
                    // Try to add the entity if it doesn't exist
                    await _userLastUsageTableClient.AddEntityAsync(tableEntity);
                    _logger.LogInformation($"Added inactive seed entity for {userEntity.UPN}");
                }
                catch (Azure.RequestFailedException ex) when (ex.Status == 409) // Conflict indicates the entity already exists
                {
                    // Merge the entity if it already exists
                    await _userLastUsageTableClient.UpdateEntityAsync(tableEntity, ETag.All, TableUpdateMode.Merge);
                }
            }
        }

        public async Task<List<InactiveUser>> GetInactiveUsers(int days)
        {
            // query to find user with more than days inactivity
            string filter = TableClient.CreateQueryFilter($"DaysSinceLastActivity eq {days}");

            // Get the users
            var users = new List<InactiveUser>();

            _logger.LogInformation($"Filter: {filter}");

            try
            {
                var queryResults = _userLastUsageTableClient.QueryAsync<TableEntity>(filter);

                await foreach (TableEntity entity in queryResults)
                {

                    users.Add(new InactiveUser
                    {
                        UPN = entity.PartitionKey,
                        DaysSinceLastActivity = entity.GetDouble("DaysSinceLastActivity") ?? 0,
                    });
                }
            }
            catch (RequestFailedException ex)
            {
                Console.WriteLine($"Error retrieving records: {ex.Message}");

            }

            return users;
        }

        public async Task<List<string>> GetUsersWithStreak(List<string> apps, int count)
        {
            // users
            var users = new List<string>();

            // build the query filter
            string filter = string.Join(" or ", apps.Select(app => $"RowKey eq '{app}' and CurrentDailyStreak eq {count}"));

            _logger.LogInformation($"Filter: {filter}");

            try
            {
                var queryResults = _userAllTimeTableClient.QueryAsync<TableEntity>(filter);

                await foreach (TableEntity entity in queryResults)
                {
                    users.Add(entity.PartitionKey);
                }
            }
            catch (RequestFailedException ex)
            {
                Console.WriteLine($"Error retrieving records: {ex.Message}");

            }

            // check we have a row for each app for each uses, group the list and check count is equal to apps count
            var groupedUsers = users.GroupBy(u => u).Select(g => new { UPN = g.Key, Count = g.Count() }).ToList();
            // filter the users to only those with a streak for all apps
            users = groupedUsers.Where(g => g.Count == apps.Count).Select(g => g.UPN).ToList();
            return users;
        }

        // public async Task<List<LeaderboardRow>> GetLeaderboard(string app, bool streak, int count)
        // {
        //     // users
        //     var users = new List<LeaderboardRow>();

        //     // build the query filter
        //     string filter = string.Join(" or ", apps.Select(app => $"RowKey eq '{app}' and CurrentDailyStreak eq {count}"));
        // }
    }
}
