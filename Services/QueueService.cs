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


            var messageObject = new CopilotQueueMessage{
                UserId = user.UPN,
                MessageText = CraftReminderMessage(user)
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

        public string CraftReminderMessage(CopilotReminderItem item)
        {
            string name = string.IsNullOrEmpty(item.DisplayName) ? "User" : item.DisplayName;
            string message = $"Hello {name},\n\n" +
                            $"We noticed that you haven't used Copilot since {item.LastActivityDate}.\n" +
                            $"It has been {item.DaysSinceLastActivity} days since your last activity.\n\n" +
                            $"Please ensure you use Copilot today to stay productive!\n\n" +
                            $"Cheers!\n" +
                            $"The Copilot Team 🚀";

            return message;
        }
    }

}