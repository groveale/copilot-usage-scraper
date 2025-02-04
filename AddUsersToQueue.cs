using System;
using groveale.Services;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace groveale
{
    public class AddUsersToQueue
    {
        private readonly ILogger _logger;
        private readonly ISettingsService _settingsService;
        private readonly ICopilotUsageSnapshotService _storageSnapshotService;
        private readonly IQueueService _queueService;

        public AddUsersToQueue(ILoggerFactory loggerFactory, ISettingsService settingsService, ICopilotUsageSnapshotService storageSnapshotService, IQueueService queueService)
        {
            _settingsService = settingsService;
            _storageSnapshotService = storageSnapshotService;
            _logger = loggerFactory.CreateLogger<AddUsersToQueue>();
            _queueService = queueService;
        }

        [Function("AddUsersToQueue")]
        public async Task Run([TimerTrigger("0 0 4 * * *")] TimerInfo myTimer)
        {
            _logger.LogInformation($"C# Timer trigger function executed at: {DateTime.Now}");
            
            if (myTimer.ScheduleStatus is not null)
            {
                _logger.LogInformation($"Next timer schedule at: {myTimer.ScheduleStatus.Next}");
            }

            // Add users to the queue
            _logger.LogInformation("Getting users to remind...");

            try
            {
                var users = await _storageSnapshotService.GetUsersForQueue();

                _logger.LogInformation($"Found {users.Count} users...");

                foreach (var user in users)
                {
                    if (user.UPN == _settingsService.ServiceAccountUpn)
                    {
                        // Skip the service account
                        continue;
                    }

                    await _queueService.AddMessageAsync(user);
                }

                _logger.LogInformation("Users added to the queue.");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error adding users to the queue. {Message}", ex.Message);
            }    

        }
    }
}
