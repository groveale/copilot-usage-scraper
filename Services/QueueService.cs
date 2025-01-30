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
            // Todo - hardcoded for demo purposes
            var messageObject = new CopilotQueueMessage{
                UserId = "5acd72ef-a0e5-455b-9a6d-8033562cf8b3",
                MessageText = "Please ensure you use Copilot today",
                chatId = "19:5acd72ef-a0e5-455b-9a6d-8033562cf8b3_fa936341-b3df-4ea2-98db-66ce0f3fbdcd@unq.gbl.spaces/messages"
            };

            string message = JsonConvert.SerializeObject(messageObject);

            // Base64 encode the message
            message = System.Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes(message));

            try {
                await _queueClient.SendMessageAsync(message);
            } catch (Exception ex) {
                Console.WriteLine($"Error adding message to queue: {ex.Message}");
            }
        }
    }

}