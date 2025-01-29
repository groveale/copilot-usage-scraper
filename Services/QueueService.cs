using Azure.Storage.Queues;
using Newtonsoft.Json;

namespace groveale.Services 
{

    public interface IQueueService
    {
        Task AddMessageAsync(CopilotReminderItem user);
    }

    public class QueueService : IQueueService
    {
        private readonly string _connectionString;
        private readonly string _queueName;

        private readonly QueueClient _queueClient;

        public QueueService(ISettingsService settingsService)
        {
            _connectionString = Environment.GetEnvironmentVariable("StorageConnectionString");
            _queueName = "copilot-reminder-queue";

            _queueClient = new QueueClient(_connectionString, _queueName);

            _queueClient.CreateIfNotExists();
        }

        public async Task AddMessageAsync(CopilotReminderItem user)
        {
            // Need to craft the message...
            // But for now, just add the users email and message to say please ensure you use Copilot today
            var messageObject = new
            {
                Email = user.UPN,
                Message = "Please ensure you use Copilot today"
            };

            string message = JsonConvert.SerializeObject(messageObject);

            try {
                await _queueClient.SendMessageAsync(message);
            } catch (Exception ex) {
                Console.WriteLine($"Error adding message to queue: {ex.Message}");
            }
        }
    }

}