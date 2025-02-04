using System;
using groveale.Services;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace groverale
{
    public class GetTodaysCopilotUsage
    {
        private readonly ILogger _logger;
        private readonly IGraphService _graphService;
        private readonly ISettingsService _settingsService;
        private readonly ICopilotUsageSnapshotService _storageSnapshotService;


        public GetTodaysCopilotUsage(ILoggerFactory loggerFactory, IGraphService graphService, ISettingsService settingsService, ICopilotUsageSnapshotService storageSnapshotService)
        {
            _graphService = graphService;
            _settingsService = settingsService;
            _storageSnapshotService = storageSnapshotService;
            _logger = loggerFactory.CreateLogger<GetTodaysCopilotUsage>();
        }

        [Function("GetTodaysCopilotUsage")]
        public async Task Run([TimerTrigger("0 0 3 * * *")] TimerInfo myTimer)
        {
            _logger.LogInformation($"C# Timer trigger function executed at: {DateTime.Now}");
            
            if (myTimer.ScheduleStatus is not null)
            {
                _logger.LogInformation($"Next timer schedule at: {myTimer.ScheduleStatus.Next}");
            }

            // Get the report settings
            var reportSettings = await _graphService.GetReportAnonSettingsAsync();

            // Set the report settings to display names (if hidden)
            if (reportSettings.DisplayConcealedNames.Value)
            {
                _logger.LogInformation("Setting report settings to display names...");
                await _graphService.SetReportAnonSettingsAsync(false);
            }
            else
            {
                _logger.LogInformation("Report settings already set to display names.");
            }
    
            // Get the usage data
            var usageData = await _graphService.GetM365CopilotUsageReportAsyncJSON(_logger);
            _logger.LogInformation($"Usage data: {usageData.Count}");

            var copilotUsers = await _graphService.GetM365CopilotUsersAsync();
            _logger.LogInformation($"copilot users: {copilotUsers.Count}");
    
            // Set the report settings back to hide names (if hidden)
            if (reportSettings.DisplayConcealedNames.Value)
            {
                _logger.LogInformation("Setting report settings back to hide names...");
                await _graphService.SetReportAnonSettingsAsync(true);
            }

            // Remove snapshot for non-copilot users
            var copilotUsageData = RemoveNonCopilotUsers(usageData, copilotUsers);
            _logger.LogInformation($"copilot users usage data: {copilotUsageData.Count}");

            try
            {
                var recordsAdded = await _storageSnapshotService.ProcessUserDailySnapshots(copilotUsageData);
                _logger.LogInformation($"Records added: {recordsAdded}");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing usage data. {Message}", ex.Message);
            }

            
 
        }

        private List<M365CopilotUsage> RemoveNonCopilotUsers(List<M365CopilotUsage> usageData, Dictionary<string, bool> copilotUsers)
        {
            var filteredUsageData = new List<M365CopilotUsage>();

            foreach (var u in usageData)
            {
                if (copilotUsers.ContainsKey(u.UserPrincipalName))
                {
                    filteredUsageData.Add(u);
                }
            }

            return filteredUsageData;
        }
    }
}
