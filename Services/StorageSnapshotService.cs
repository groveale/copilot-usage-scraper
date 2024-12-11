using System.Text.Json;
using Azure;
using Azure.Data.Tables;
using Microsoft.Extensions.Options;

namespace groveale.Services 
{
    public interface IStorageSnapshotService
    {
        Task<int> ProcessUserDailySnapshots(List<M365CopilotUsage> siteSnapshots);
    }

    public class StorageSnapshotService : IStorageSnapshotService
    {
        private readonly TableServiceClient _serviceClient;
        private readonly bool CDXTenant = System.Environment.GetEnvironmentVariable("CDXTenant") == "true";
        private readonly string _userDAUTableName = "CopilotUsageDailySnapshots";
        public StorageSnapshotService()
        {
            var storageUri = System.Environment.GetEnvironmentVariable("StorageAccountUri");
            var accountName = System.Environment.GetEnvironmentVariable("StorageAccountName");
            var storageAccountKey = System.Environment.GetEnvironmentVariable("StorageAccountKey");


            _serviceClient = new TableServiceClient(
                new Uri(storageUri),
                new TableSharedKeyCredential(accountName, storageAccountKey));
        }

        public async Task<int> ProcessUserDailySnapshots(List<M365CopilotUsage> userSnapshots)
        {
            int DAUadded = 0;

            var tableClient = _serviceClient.GetTableClient(_userDAUTableName);
            tableClient.CreateIfNotExists();

            foreach (var userSnap in userSnapshots)
            {

                var userEntity = ConvertToUserActivity(userSnap);
                if (userEntity == null)
                {
                    continue;
                }

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
            

            return DAUadded;
            
        }

        public UserActivity ConvertToUserActivity(M365CopilotUsage user)
        {
            // if last activity date is not the same as the report refresh date, we don't want to add it
            // there is no daily activity if the last activity date is not the same as the report refresh date

            if (user.LastActivityDate != user.ReportRefreshDate)
            {    
                if (!CDXTenant)
                {
                    return null;
                }
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
