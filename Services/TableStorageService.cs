using System.Text.Json;
using Azure;
using Azure.Data.Tables;
using Microsoft.Extensions.Options;

namespace groveale.Services 
{
    public interface ICopilotUsageSnapshotService
    {
        Task<int> ProcessUserDailySnapshots(List<M365CopilotUsage> siteSnapshots);

        Task<List<CopilotReminderItem>> GetUsersForQueue();
    }

    public class CopilotUsageSnapshotService : ICopilotUsageSnapshotService
    {
        private readonly TableServiceClient _serviceClient;
        private readonly bool CDXTenant = System.Environment.GetEnvironmentVariable("CDXTenant") == "true";
        private readonly string _userDAUTableName = "CopilotUsageDailySnapshots";
        private readonly string _userLastUsageTableName = "UsersLastUsageTracker";

        private readonly int _daysToCheck = int.TryParse(System.Environment.GetEnvironmentVariable("ReminderDays"), out var days) ? days : 0;

        private readonly int reminderInterval = int.TryParse(System.Environment.GetEnvironmentVariable("ReminderInterval"), out var date) ? date : 0;

        private readonly int reminderCount = int.TryParse(System.Environment.GetEnvironmentVariable("ReminderCount"), out var date) ? date : 0;
        
        public CopilotUsageSnapshotService()
        {
            var storageUri = System.Environment.GetEnvironmentVariable("StorageAccountUri");
            var accountName = System.Environment.GetEnvironmentVariable("StorageAccountName");
            var storageAccountKey = System.Environment.GetEnvironmentVariable("StorageAccountKey");


            _serviceClient = new TableServiceClient(
                new Uri(storageUri),
                new TableSharedKeyCredential(accountName, storageAccountKey));
        }

        public async Task<List<CopilotReminderItem>> GetUsersForQueue()
        {
            // Get users from the DB that need a notification
            var tableClient = _serviceClient.GetTableClient(_userLastUsageTableName);
            tableClient.CreateIfNotExists();



            // Define the query filter
            // todo need to also add days since usage (all users will be here)
            string filter = TableClient.CreateQueryFilter(
                $"(DaysSinceLastNotification gt {reminderInterval} or DaysSinceLastNotification eq null) and (NotificationCount lt {reminderCount} or NotificationCount eq null)"
            );

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
                        LastActivityDate = entity["LastActivityDate"].ToString(),
                        DaysSinceLastActivity = (double)entity["DaysSinceLastActivity"],
                        DaysSinceLastNotification = entity["DaysSinceLastNotification"] != null ? (int)entity["DaysSinceLastNotification"] : 0,
                        NotificationCount = entity["NotificationCount"] != null ? (int)entity["NotificationCount"] : 0,
                        DisplayName = entity["DisplayName"].ToString(),
                        
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

            var tableClient = _serviceClient.GetTableClient(_userDAUTableName);
            tableClient.CreateIfNotExists();

            foreach (var userSnap in userSnapshots)
            {
                // if last activity date is not the same as the report refresh date, we need to validate how long usage has not occured
                // there is no daily activity if the last activity date is not the same as the report refresh date

                if (userSnap.LastActivityDate != userSnap.ReportRefreshDate)
                {    
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
                        var reportRefreshDate = DateTime.ParseExact(userSnap.ReportRefreshDate, "yyyy-MM-dd", null);

                        // Check if last activity is before days ti check
                        if (lastActivityDate.AddDays(_daysToCheck) < reportRefreshDate)
                        {
                            // we need to record in another table
                            lastActivityDates.Add((userSnap.LastActivityDate, userSnap.UserPrincipalName, userSnap.ReportRefreshDate, userSnap.DisplayName));
                        }
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
                    { "DailyCopilotChatActivity", userEntity.DailyCopilotChatActivity }
                };        

                try
                {
                    // Try to add the entity if it doesn't exist
                    await tableClient.AddEntityAsync(tableEntity);
                    DAUadded++;
                }
                catch (Azure.RequestFailedException ex) when (ex.Status == 409) // Conflict indicates the entity already exists
                {
                    // Merge the entity if it already exists
                    await tableClient.UpdateEntityAsync(tableEntity, ETag.All, TableUpdateMode.Merge);
                }
            }
            
            // Do we have any users to record in the last activity table?
            if (lastActivityDates.Count > 0)
            {
                var lastActivityTableClient = _serviceClient.GetTableClient(_userLastUsageTableName);
                lastActivityTableClient.CreateIfNotExists();

                var today = DateTime.UtcNow.ToString("yyyy-MM-dd");

                foreach (var (lastActivityDate, userPrincipalName, reportRefreshDate, displayName) in lastActivityDates)
                {

                    // Check if record exists in table for user
                    var tableEntity = new TableEntity(userPrincipalName, lastActivityDate)
                    {
                        { "LastActivityDate", lastActivityDate },
                        { "ReportRefreshDate", reportRefreshDate },
                        { "DaysSinceLastActivity", (DateTime.ParseExact(reportRefreshDate, "yyyy-MM-dd", null) - DateTime.ParseExact(lastActivityDate, "yyyy-MM-dd", null)).TotalDays },
                        { "LastNotificationDate", today },
                        { "DaysSinceLastNotification", null },
                        { "NotificationCount", null },
                        { "DisplayName", displayName }
                    };

                    try
                    {
                        // Try to add the entity if it doesn't exist
                        await lastActivityTableClient.AddEntityAsync(tableEntity);
                    }
                    catch (Azure.RequestFailedException ex) when (ex.Status == 409) // Conflict indicates the entity already exists
                    {
                        // Merge the entity if it already exists
                        // Get the existing entity
                        var existingTableEntity = await lastActivityTableClient.GetEntityAsync<TableEntity>(userPrincipalName, lastActivityDate);

                        // Need to up the reminder count
                        tableEntity["NotificationCount"] = existingTableEntity.Value["NotificationCount"] != null ? (int)existingTableEntity.Value["NotificationCount"] + 1 : 1;

                        // Need to up the days since last notification
                        tableEntity["DaysSinceLastNotification"] = (DateTime.ParseExact(today, "yyyy-MM-dd", null) - DateTime.ParseExact(existingTableEntity.Value["LastNotificationDate"].ToString(), "yyyy-MM-dd", null)).TotalDays;

                        await lastActivityTableClient.UpdateEntityAsync(tableEntity, ETag.All, TableUpdateMode.Merge);
                    }
                }
            
                
                
            }

            // Find all records with report refresh date note equal to current and delete
            // Clear out users who have had activity since we last reminded them
            string filter = TableClient.CreateQueryFilter($"ReportRefreshDate ne '{userSnapshots[0].ReportRefreshDate}'");
            var queryResults = tableClient.QueryAsync<TableEntity>(filter);

            await foreach (TableEntity entity in queryResults)
            {
                await tableClient.DeleteEntityAsync(entity.PartitionKey, entity.RowKey);
            }

            return DAUadded;
            
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
            
            // it's a mere string comparison
            bool DailyUsage(string lastActivityDate, string reportRefreshDate)
            {
                return lastActivityDate == reportRefreshDate;
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
                DailyCopilotChatActivity = DailyUsage(user.CopilotChatLastActivityDate, user.ReportRefreshDate)
            };
        }

    }
}
