public class CopilotReminderItem
{
    public string UPN { get; set; }
    public string DisplayName { get; set; }
    public string LastActivityDate { get; set; }
    public double DaysSinceLastActivity { get; set; }
    public double DaysSinceLastNotification { get; set; }
    public int NotificationCount { get; set; }
}