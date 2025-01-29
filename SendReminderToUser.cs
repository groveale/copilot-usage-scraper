using System;
using Azure.Storage.Queues.Models;
using groveale.Services;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace groveale
{
    public class SendReminderToUser
    {
        private readonly ILogger<SendReminderToUser> _logger;
        private readonly IGraphService _graphService;

        public SendReminderToUser(ILogger<SendReminderToUser> logger, IGraphService graphService)
        {
            _logger = logger;
            _graphService = graphService;
        }

        [Function(nameof(SendReminderToUser))]
        public async Task Run([QueueTrigger("copilot-reminder-queue", Connection = "StorageConnectionString")] QueueMessage message)
        {
            _logger.LogInformation($"C# Queue trigger function processed: {message.MessageText}");

            // Send Teams message to user
            _logger.LogInformation("Sending message to user...");

            // Serialize the message to get the user email
            var queueMessage = JsonConvert.DeserializeObject<QueueMessage>(message.MessageText);

            if (String.IsNullOrWhiteSpace(queueMessage.chatId))
            {
                // Create a chat with the user
                queueMessage.chatId = await _graphService.CreateChatAsync(queueMessage.UserId);
            }

            // Send message
            await _graphService.SendChatMessageToUserAsync(queueMessage.MessageText, queueMessage.chatId);

        }
    }
}
