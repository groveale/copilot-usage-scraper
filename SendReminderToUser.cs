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
        private readonly IGraphDelegatedService _graphDelegatedService;

        public SendReminderToUser(ILogger<SendReminderToUser> logger, IGraphDelegatedService graphDelegatedService)
        {
            _logger = logger;
            _graphDelegatedService = graphDelegatedService;
        }

        [Function(nameof(SendReminderToUser))]
        public async Task Run([QueueTrigger("copilot-reminder-queue", Connection = "StorageConnectionString")] QueueMessage message)
        {
            _logger.LogInformation($"C# Queue trigger function processed: {message.MessageText}");


            // Send Teams message to user
            _logger.LogInformation("Sending message to user...");

            // Serialize the message to get the user email
            var queueMessage = JsonConvert.DeserializeObject<CopilotQueueMessage>(message.MessageText);

            var chatId = await _graphDelegatedService.CreateChatAsync(queueMessage.UserId);

            if (string.IsNullOrEmpty(chatId))
            {
                _logger.LogError("Error creating chat with user.");
            }
            
            // Send message
            await _graphDelegatedService.SendChatMessageToUserAsync(queueMessage.MessageText, chatId);

        }
    }
}
