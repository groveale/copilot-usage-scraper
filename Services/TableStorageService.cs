using System.Text.Json;
using Azure;
using Azure.Data.Tables;
using groveale.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Graph.Beta.Models;

namespace groveale.Services
{
    public interface ICopilotUsageSnapshotService
    {
        Task<int> ProcessUserDailySnapshots(List<M365CopilotUsage> siteSnapshots, DeterministicEncryptionService encryptionService);

        Task<List<CopilotReminderItem>> GetUsersForQueue();

        Task ResetUsersAppStreak(AppType appType, string upn);

        Task<string?> GetStartDate(string timeFrame);

        Task<List<string>> GetUsersWhoHaveCompletedActivity(List<string> apps, string count, string timeFrame, string startDate);
        Task<List<string>> GetUsersWhoHaveCompletedActivity(string app, string count, string timeFrame, string startDate);

        //Task<List<LeaderboardRow>> GetLeaderboard(string app, bool streak, int count);

        Task<List<InactiveUser>> GetInactiveUsers(int days);
        Task<List<string>> GetUsersWithStreak(List<string> apps, int count);

        Task<List<string>> GetUsersWithStreakForApp(string app, int count);

        // For Seeding
        Task SeedDailyActivitiesAsync(List<UserActivity> userActivitiesSeed);
        Task SeedMonthlyFrameActivitiesAsync(List<CopilotTimeFrameUsage> userActivitiesSeed, string startDate);
        Task SeedWeeklyTimeFrameActivitiesAsync(List<CopilotTimeFrameUsage> userActivitiesSeed, string startDate);
        Task SeedAllTimeActivityAsync(List<CopilotTimeFrameUsage> userActivitiesSeed);
        Task SeedInactiveUsersAsync(List<InactiveUser> inactiveUsersSeed);
        Task<List<string>> GetUsersWhoHaveCompletedActivityForApp(string app, string? dayCount, string? interactionCount, string timeFrame, string? startDate);
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
        private readonly TableClient _agentInteractionDailyAggregationForUserTableClient;
        private readonly TableClient _agentWeeklyTableClient;
        private readonly TableClient _agentMonthlyTableClient;
        private readonly TableClient _agentAllTimeTableClient;
        private readonly TableClient _agentAllTimeByUserTableClient;
        private readonly TableClient _agentWeeklyByUserTableClient;
        private readonly TableClient _agentMonthlyByUserTableClient;
        private readonly ILogger<CopilotUsageSnapshotService> _logger;
        private readonly bool CDXTenant = System.Environment.GetEnvironmentVariable("CDXTenant") == "true";
        private readonly string _userDAUTableName = "CopilotUsageDailySnapshots";
        private readonly string _userLastUsageTableName = "UsersLastUsageTracker";
        private readonly string _userWeeklyTableName = "CopilotUsageWeeklySnapshots1";
        private readonly string _agentWeeklyTableName = "AgentUsageWeeklySnapshots";
        private readonly string _agentWeeklyByUserTableName = "AgentUsageWeeklyByUserSnapshots";
        private readonly string _userMonthlyTableName = "CopilotUsageMonthlySnapshots1";
        private readonly string _agentMonthlyTableName = "AgentUsageMonthlySnapshots";
        private readonly string _agentMonthlyByUserTableName = "AgentUsageMonthlyByUserSnapshots";
        private readonly string _userAllTimeTableName = "CopilotUsageAllTimeRecord1";
        private readonly string _agentAllTimeTableName = "AgentUsageAllTimeRecord";
        private readonly string _agentAllTimeByUserTableName = "AgentUsageAllTimeByUserRecord";
        private readonly string _reportRefreshDateTableName = "ReportRefreshRecord";
        //TODO: remove the 3
        private readonly string _copilotInteractionDailyAggregationTableName = "CopilotInteractionDailyAggregationByAppAndUser3";
        private readonly string _agentInteractionDailyAggregationForUserTable = "AgentInteractionDailyAggregationByUserAndAgentId";

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

            _agentInteractionDailyAggregationForUserTableClient = _serviceClient.GetTableClient(_agentInteractionDailyAggregationForUserTable);
            _agentInteractionDailyAggregationForUserTableClient.CreateIfNotExists();

            _agentWeeklyTableClient = _serviceClient.GetTableClient(_agentWeeklyTableName);
            _agentWeeklyTableClient.CreateIfNotExists();
            _agentMonthlyTableClient = _serviceClient.GetTableClient(_agentMonthlyTableName);
            _agentMonthlyTableClient.CreateIfNotExists();
            _agentAllTimeTableClient = _serviceClient.GetTableClient(_agentAllTimeTableName);
            _agentAllTimeTableClient.CreateIfNotExists();

            _agentAllTimeByUserTableClient = _serviceClient.GetTableClient(_agentAllTimeByUserTableName);
            _agentAllTimeByUserTableClient.CreateIfNotExists();
            _agentWeeklyByUserTableClient = _serviceClient.GetTableClient(_agentWeeklyByUserTableName);
            _agentWeeklyByUserTableClient.CreateIfNotExists();
            _agentMonthlyByUserTableClient = _serviceClient.GetTableClient(_agentMonthlyByUserTableName);
            _agentMonthlyByUserTableClient.CreateIfNotExists();

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

        public async Task<int> ProcessUserDailySnapshots(List<M365CopilotUsage> userSnapshots, DeterministicEncryptionService encryptionService)
        {
            int DAUadded = 0;

            // Tuple to store the user's last activity date and username
            var lastActivityDates = new List<(string, string, string, string)>();

            string reportRefreshDateString = string.Empty;



            foreach (var userSnap in userSnapshots)
            {
                reportRefreshDateString = userSnap.ReportRefreshDate;
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

                }

                // Encrypt the UPN - lookup is already encrypted
                userSnap.UserPrincipalName = encryptionService.Encrypt(userSnap.UserPrincipalName);

                // if getting audit data we should get the aggregation data for the user
                // Get the aggregation entity
                var aggregationEntity = await GetDailyAuditDataForUser(userSnap.UserPrincipalName, userSnap.ReportRefreshDate);

                var userEntity = ConvertToUserActivity(userSnap, aggregationEntity);

                // Get User Activity
                // Todo, store the more precise data in the table
                var userActivityDictionary = ConvertToUsageDictionary(userEntity);


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

                // Agent Data
                var dailyAgentData = await GetDailyAgentDataForUser(userEntity.UPN, userEntity.ReportDate.ToString("yyyy-MM-dd"));





                // We need to update the  weekly, monthly and alltime tables
                await UpdateUserSnapshots(userActivityDictionary, userEntity.UPN, "alltime", userEntity.ReportDate.ToString("yyyy-MM-dd"));
                await UpdateAgentSnapshots(dailyAgentData, "alltime", userEntity.UPN, userEntity.ReportDate.ToString("yyyy-MM-dd"));

                // Update Monthly
                var firstOfMonthForSnapshot = GetMonthStartDate(userEntity.ReportDate);

                await UpdateUserSnapshots(userActivityDictionary, userEntity.UPN, "monthly", firstOfMonthForSnapshot);
                await UpdateAgentSnapshots(dailyAgentData, "monthly", userEntity.UPN, firstOfMonthForSnapshot);

                // Update Weekly - Get our timeFrame
                var firstMondayOfWeeklySnapshot = GetWeekStartDate(userEntity.ReportDate);

                await UpdateUserSnapshots(userActivityDictionary, userEntity.UPN, firstMondayOfWeeklySnapshot, firstMondayOfWeeklySnapshot);
                await UpdateAgentSnapshots(dailyAgentData, "monthly", userEntity.UPN, firstMondayOfWeeklySnapshot);

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
                    // Encrypt the UPN
                    var encryptedUPN = encryptionService.Encrypt(userPrincipalName);

                    // Check if record exists in table for user
                    var tableEntity = new TableEntity(encryptedUPN, lastActivityDate)
                    {
                        { "LastActivityDate", lastActivityDate },
                        { "ReportRefreshDate", reportRefreshDateItem },
                        { "DaysSinceLastActivity", (DateTime.ParseExact(reportRefreshDateItem, "yyyy-MM-dd", null) - DateTime.ParseExact(lastActivityDate, "yyyy-MM-dd", null)).TotalDays },
                        // { "LastNotificationDate", today },
                        // { "DaysSinceLastNotification", (double)999 },
                        // { "NotificationCount", 0 },
                    };

                    try
                    {
                        // Try to add the entity if it doesn't exist
                        await _userLastUsageTableClient.AddEntityAsync(tableEntity);
                    }
                    catch (Azure.RequestFailedException ex) when (ex.Status == 409) // Conflict indicates the entity already exists
                    {
                        await _userLastUsageTableClient.UpdateEntityAsync(tableEntity, ETag.All, TableUpdateMode.Merge);
                    }
                }
            }

            return DAUadded;

        }

        private async Task<List<AgentInteraction>> GetDailyAgentDataForUser(string uPN, string reportDate)
        {
            List<AgentInteraction> agentInteractions = new List<AgentInteraction>();

            try
            {
                // Query by partition key only
                string filter = $"PartitionKey eq '{reportDate}-{uPN}'";
                var queryResults = _agentInteractionDailyAggregationForUserTableClient.QueryAsync<AgentInteraction>(filter);

                // Process the results
                await foreach (AgentInteraction entity in queryResults)
                {
                    // Process each entity as needed
                    agentInteractions.Add(entity);
                }
            }
            catch (RequestFailedException ex) when (ex.Status == 404)
            {
                _logger.LogWarning($"No entity found for UPN: {uPN}, ReportDate: {reportDate}. Returning null.");
            }
            catch (RequestFailedException ex)
            {
                _logger.LogError($"Failed to retrieve entity for UPN: {uPN}, ReportDate: {reportDate}. Error: {ex.Message}");

            }

            return agentInteractions;
        }


        private async Task<List<CopilotTimeFrameUsage>> GetDailyAuditDataForUser(string uPN, string reportDate)
        {
            List<CopilotTimeFrameUsage> copilotInteractions = new List<CopilotTimeFrameUsage>();

            try
            {
                // Query by partition key only
                string filter = $"PartitionKey eq '{reportDate}-{uPN}'";
                var queryResults = _copilotInteractionDailyAggregationTableClient.QueryAsync<CopilotTimeFrameUsage>(filter);

                // Process the results
                await foreach (CopilotTimeFrameUsage entity in queryResults)
                {
                    // Process each entity as needed
                    copilotInteractions.Add(new CopilotTimeFrameUsage
                    {
                        UPN = uPN,
                        App = Enum.TryParse<AppType>(entity.RowKey, out var appType) ? appType : default,
                        TotalDailyActivityCount = entity.TotalDailyActivityCount,
                        TotalInteractionCount = entity.TotalInteractionCount
                    });
                }
            }
            catch (RequestFailedException ex) when (ex.Status == 404)
            {
                _logger.LogWarning($"No entity found for UPN: {uPN}, ReportDate: {reportDate}. Returning null.");
            }
            catch (RequestFailedException ex)
            {
                _logger.LogError($"Failed to retrieve entity for UPN: {uPN}, ReportDate: {reportDate}. Error: {ex.Message}");
            }

            return copilotInteractions;
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
                var existingTableEntity = await _userAllTimeTableClient.GetEntityAsync<CopilotTimeFrameUsage>(upn, appType.ToString());
                // Perform your logic here with existingTableEntity

                existingTableEntity.Value.CurrentDailyStreak = 0;

                await _userAllTimeTableClient.UpdateEntityAsync(existingTableEntity.Value, ETag.All, TableUpdateMode.Merge);

            }
            catch (RequestFailedException ex) when (ex.Status == 404)
            {
                // Entity not found - nothing to reset
            }

        }

        private async Task UpdateAgentSnapshots(List<AgentInteraction> usersDailyAgentActivity, string timeFrame, string upn, string startDate)
        {
            TableClient tableClientByUser = _agentAllTimeByUserTableClient;
            string partitionKey = string.Empty;

            // get the table
            switch (timeFrame)
            {
                case "weekly":

                    tableClientByUser = _agentWeeklyByUserTableClient;
                    partitionKey = startDate;
                    break;
                case "monthly":
                    tableClientByUser = _agentMonthlyByUserTableClient;
                    partitionKey = startDate;
                    break;
                default:
                    partitionKey = AgentInteraction.AllTimePartitionKeyPrefix;
                    break;
            }

            foreach (var agentDailyInteractions in usersDailyAgentActivity)
            {
                int interactionCount = agentDailyInteractions.TotalInteractionCount;

                // At this point row key is the agentId
                var agentPartitionKey = $"{partitionKey}-{agentDailyInteractions.RowKey}";

                try
                {
                    // first Update the users all time usage of the agent - 
                    var existingTableEntity = await tableClientByUser.GetEntityAsync<AgentInteraction>(agentPartitionKey, upn);

                    // Increment the daily all time activity count
                    existingTableEntity.Value.TotalDailyActivityCount = existingTableEntity.Value.TotalDailyActivityCount + 1;
                    existingTableEntity.Value.TotalInteractionCount = existingTableEntity.Value.TotalInteractionCount + interactionCount;

                    await _userAllTimeTableClient.UpdateEntityAsync(existingTableEntity.Value, ETag.All, TableUpdateMode.Merge);

                }
                catch (RequestFailedException ex) when (ex.Status == 404)
                {

                    // if usage create first item
                    var newAllTimeUsage = new AgentInteraction()
                    {
                        UPN = upn,
                        AgentId = agentDailyInteractions.RowKey,
                        AgentName = agentDailyInteractions.AgentName,
                        TotalDailyActivityCount = 1,
                        TotalInteractionCount = interactionCount
                    };

                    if (timeFrame == "alltime")
                    {
                        await _userAllTimeTableClient.AddEntityAsync(newAllTimeUsage.ToAllTimeTableEntity());
                    }
                    else
                    {
                        await _userAllTimeTableClient.AddEntityAsync(newAllTimeUsage.ToTimeFrameTableEntity(startDate));
                    }

                }
                catch (RequestFailedException ex)
                {
                    Console.WriteLine($"Error updating agent records: {ex.Message}");
                }
            }
        }
        private async Task UpdateUserSnapshots(Dictionary<AppType, Tuple<bool, int>> userActivityDictionary, string upn, string timeFrame, string startDate)
        {
            // get the table
            var tableClient = _userAllTimeTableClient;
            string partitionKey = $"{CopilotTimeFrameUsage.AllTimePartitionKeyPrefix}-{upn}";

            if (timeFrame == "weekly")
            {
                tableClient = _userWeeklyTableClient;
                partitionKey = $"{startDate}-{upn}";
            }
            else if (timeFrame == "monthly")
            {
                tableClient = _userMonthlyTableClient;
                partitionKey = $"{startDate}-{upn}";
            }

            foreach (var (app, tuple) in userActivityDictionary)
            {
                bool dailyUsage = tuple.Item1;
                int interactionCount = tuple.Item2;

                try
                {
                    // Get the existing entity
                    var existingTableEntity = await tableClient.GetEntityAsync<CopilotTimeFrameUsage>(partitionKey, app.ToString());

                    // If usage 
                    if (dailyUsage)
                    {
                        // Increment the daily all time activity count
                        existingTableEntity.Value.TotalDailyActivityCount = existingTableEntity.Value.TotalDailyActivityCount + 1;
                        existingTableEntity.Value.CurrentDailyStreak = existingTableEntity.Value.CurrentDailyStreak + 1;
                        existingTableEntity.Value.BestDailyStreak = Math.Max(existingTableEntity.Value.BestDailyStreak, existingTableEntity.Value.CurrentDailyStreak);
                        existingTableEntity.Value.TotalInteractionCount = existingTableEntity.Value.TotalInteractionCount + interactionCount;

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

                    // Only create if usage
                    if (dailyUsage)
                    {
                        var newAllTimeUsage = new CopilotTimeFrameUsage()
                        {
                            UPN = upn,
                            App = app,
                            TotalDailyActivityCount = 1,
                            TotalInteractionCount = interactionCount,
                            CurrentDailyStreak = 1,
                            BestDailyStreak = 1
                        };

                        if (timeFrame == "alltime")
                        {
                            await _userAllTimeTableClient.AddEntityAsync(newAllTimeUsage.ToAllTimeTableEntity());
                        }
                        else
                        {
                            await _userAllTimeTableClient.AddEntityAsync(newAllTimeUsage.ToTimeFrameTableEntity(startDate));
                        }
                    }

                }
                catch (RequestFailedException ex)
                {
                    Console.WriteLine($"Error updating records: {ex.Message}");
                }
            }
        }

        private UserActivity ConvertToUserActivity(M365CopilotUsage user, List<CopilotTimeFrameUsage> aggregationUsageList)
        {
            // if no aggregation entity, we have no usage
            if (aggregationUsageList.Count == 0)
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
                    DailyAllUpActivity = false,
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
                    DailyWebPluginInteractions = 0,
                    DailyAgentActivity = false,
                    DailyAgentInteractions = 0
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

            // Turn the aggregation list into a dictionary with app as the key
            // and the value as interaction count
            var aggregationUsageDict = aggregationUsageList.ToDictionary(x => x.App, x => x.TotalInteractionCount);

            int DailyInteractionCountForApp(AppType appType)
            {
                if (aggregationUsageDict.TryGetValue(appType, out var interactionCount))
                {
                    return interactionCount;
                }
                return 0;
            }



            return new UserActivity
            {
                ReportDate = DateTime.SpecifyKind(DateTime.ParseExact(user.ReportRefreshDate, "yyyy-MM-dd", null), DateTimeKind.Utc),
                UPN = user.UserPrincipalName,
                DisplayName = user.DisplayName,
                DailyTeamsActivity = DailyUsage(user.MicrosoftTeamsCopilotLastActivityDate, user.ReportRefreshDate),
                DailyTeamsInteractionCount = DailyInteractionCountForApp(AppType.Teams),
                DailyOutlookActivity = DailyUsage(user.OutlookCopilotLastActivityDate, user.ReportRefreshDate),
                DailyOutlookInteractionCount = DailyInteractionCountForApp(AppType.Outlook),
                DailyWordActivity = DailyUsage(user.WordCopilotLastActivityDate, user.ReportRefreshDate),
                DailyWordInteractionCount = DailyInteractionCountForApp(AppType.Word),
                DailyExcelActivity = DailyUsage(user.ExcelCopilotLastActivityDate, user.ReportRefreshDate),
                DailyExcelInteractionCount = DailyInteractionCountForApp(AppType.Excel),
                DailyPowerPointActivity = DailyUsage(user.PowerPointCopilotLastActivityDate, user.ReportRefreshDate),
                DailyPowerPointInteractionCount = DailyInteractionCountForApp(AppType.PowerPoint),
                DailyOneNoteActivity = DailyUsage(user.OneNoteCopilotLastActivityDate, user.ReportRefreshDate),
                DailyOneNoteInteractionCount = DailyInteractionCountForApp(AppType.OneNote),
                DailyLoopActivity = DailyUsage(user.LoopCopilotLastActivityDate, user.ReportRefreshDate),
                DailyLoopInteractionCount = DailyInteractionCountForApp(AppType.Loop),
                DailyCopilotChatActivity = DailyUsage(user.CopilotChatLastActivityDate, user.ReportRefreshDate),
                DailyCopilotChatInteractionCount = DailyInteractionCountForApp(AppType.CopilotChat),
                DailyAllInteractionCount = DailyInteractionCountForApp(AppType.All),
                DailyMACActivity = DailyUsageFromCount(DailyInteractionCountForApp(AppType.MAC)),
                DailyMACInteractionCount = DailyInteractionCountForApp(AppType.MAC),
                DailyDesignerActivity = DailyUsageFromCount(DailyInteractionCountForApp(AppType.Designer)),
                DailyDesignerInteractionCount = DailyInteractionCountForApp(AppType.Designer),
                DailySharePointActivity = DailyUsageFromCount(DailyInteractionCountForApp(AppType.SharePoint)),
                DailySharePointInteractionCount = DailyInteractionCountForApp(AppType.SharePoint),
                DailyPlannerActivity = DailyUsageFromCount(DailyInteractionCountForApp(AppType.Planner)),
                DailyPlannerInteractionCount = DailyInteractionCountForApp(AppType.Planner),
                DailyWhiteboardActivity = DailyUsageFromCount(DailyInteractionCountForApp(AppType.Whiteboard)),
                DailyWhiteboardInteractionCount = DailyInteractionCountForApp(AppType.Whiteboard),
                DailyStreamActivity = DailyUsageFromCount(DailyInteractionCountForApp(AppType.Stream)),
                DailyStreamInteractionCount = DailyInteractionCountForApp(AppType.Stream),
                DailyFormsActivity = DailyUsageFromCount(DailyInteractionCountForApp(AppType.Forms)),
                DailyFormsInteractionCount = DailyInteractionCountForApp(AppType.Forms),
                DailyCopilotActionActivity = DailyUsageFromCount(DailyInteractionCountForApp(AppType.CopilotAction)),
                DailyCopilotActionCount = DailyInteractionCountForApp(AppType.CopilotAction),
                DailyWebPluginActivity = DailyUsageFromCount(DailyInteractionCountForApp(AppType.WebPlugin)),
                DailyWebPluginInteractions = DailyInteractionCountForApp(AppType.WebPlugin),
                DailyAgentActivity = DailyUsageFromCount(DailyInteractionCountForApp(AppType.Agent)),
                DailyAgentInteractions = DailyInteractionCountForApp(AppType.Agent),

                DailyAllUpActivity = copilotAllUpActivity
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
                { AppType.All, new Tuple<bool,int>(userActivity.DailyAllUpActivity, userActivity.DailyAllInteractionCount) },
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

        public async Task<List<string>> GetUsersWhoHaveCompletedActivityForApp(string app, string? dayCount, string? interactionCount, string timeFrame, string date)
        {
            // switch statement to get the correct table on timeFrame
            var tableClient = timeFrame.ToLowerInvariant() switch
            {
                "weekly" => _userWeeklyTableClient,
                "monthly" => _userMonthlyTableClient,
                "alltime" => _userAllTimeTableClient,
                _ => throw new ArgumentException("Invalid timeFrame")
            };

            // Define the query filter
            string filter = $"PartitionKey eq '{date}-{app}'";

            if (!string.IsNullOrEmpty(dayCount))
            {
                filter += $" and TotalDailyActivityCount ge {dayCount}";
            }

            if (!string.IsNullOrEmpty(interactionCount))
            {
                filter += $" and TotalInteractionCount ge {interactionCount}";
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
                // Add user recor

                try
                {
                    // Try to add the entity if it doesn't exist
                    await _userDAUTableClient.AddEntityAsync(userEntity.ToTableEntity());
                    _logger.LogInformation($"Added daily seed entity for {userEntity.UPN}");
                }
                catch (Azure.RequestFailedException ex) when (ex.Status == 409) // Conflict indicates the entity already exists
                {
                    // Merge the entity if it already exists
                    await _userDAUTableClient.UpdateEntityAsync(userEntity.ToTableEntity(), ETag.All, TableUpdateMode.Merge);
                }
            }

        }


        public async Task SeedMonthlyFrameActivitiesAsync(List<CopilotTimeFrameUsage> userActivitiesSeed, string startDate)
        {
            // Get daily table
            foreach (var userEntity in userActivitiesSeed)
            {
                try
                {
                    // Try to add the entity if it doesn't exist
                    await _userMonthlyTableClient.AddEntityAsync(userEntity.ToTimeFrameTableEntity(startDate));
                    _logger.LogInformation($"Added monthly seed entity for {userEntity.UPN}");
                }
                catch (Azure.RequestFailedException ex) when (ex.Status == 409) // Conflict indicates the entity already exists
                {
                    // Merge the entity if it already exists
                    await _userMonthlyTableClient.UpdateEntityAsync(userEntity.ToTimeFrameTableEntity(startDate), ETag.All, TableUpdateMode.Merge);
                }
            }
        }

        public async Task SeedWeeklyTimeFrameActivitiesAsync(List<CopilotTimeFrameUsage> userActivitiesSeed, string startDate)
        {
            // Get daily table
            foreach (var userEntity in userActivitiesSeed)
            {
                try
                {
                    // Try to add the entity if it doesn't exist
                    await _userWeeklyTableClient.AddEntityAsync(userEntity.ToTimeFrameTableEntity(startDate));
                    _logger.LogInformation($"Added weekly seed entity for {userEntity.UPN}");
                }
                catch (Azure.RequestFailedException ex) when (ex.Status == 409) // Conflict indicates the entity already exists
                {
                    // Merge the entity if it already exists
                    await _userWeeklyTableClient.UpdateEntityAsync(userEntity.ToTimeFrameTableEntity(startDate), ETag.All, TableUpdateMode.Merge);
                }
            }
        }

        public async Task SeedAllTimeActivityAsync(List<CopilotTimeFrameUsage> userActivitiesSeed)
        {
            // Get daily table
            foreach (var userEntity in userActivitiesSeed)
            {
                try
                {
                    // Try to add the entity if it doesn't exist
                    await _userAllTimeTableClient.AddEntityAsync(userEntity.ToAllTimeTableEntity());
                    _logger.LogInformation($"Added alltime seed entity for {userEntity.UPN}");
                }
                catch (Azure.RequestFailedException ex) when (ex.Status == 409) // Conflict indicates the entity already exists
                {
                    // Merge the entity if it already exists
                    await _userAllTimeTableClient.UpdateEntityAsync(userEntity.ToAllTimeTableEntity(), ETag.All, TableUpdateMode.Merge);
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

        public async Task<List<string>> GetUsersWithStreakForApp(string app, int count)
        {
            // users
            var users = new List<string>();

            // build the query filter
            string filter = $"PartitionKey eq '{CopilotTimeFrameUsage.AllTimePartitionKeyPrefix}-{app}' and CurrentDailyStreak ge {count}";

            _logger.LogInformation($"Filter: {filter}");

            try
            {
                var queryResults = _userAllTimeTableClient.QueryAsync<TableEntity>(filter);

                await foreach (TableEntity entity in queryResults)
                {
                    users.Add(entity.RowKey);
                }
            }
            catch (RequestFailedException ex)
            {
                Console.WriteLine($"Error retrieving records: {ex.Message}");

            }

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
