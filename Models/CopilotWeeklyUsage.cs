public class WeeklyUsage
{
    // Allways a Monday on the week
    public DateTime StartDate { get; set; }
    public string UPN { get; set; }
    public string DisplayName { get; set; }
    public int DailyTeamsActivityCount { get; set; }
    public int DailyCopilotChatActivityCount { get; set; }
    public int DailyOutlookActivityCount { get; set; }
    public int DailyWordActivityCount { get; set; }
    public int DailyExcelActivityCount { get; set; }
    public int DailyPowerPointActivityCount { get; set; }
    public int DailyOneNoteActivityCount { get; set; }
    public int DailyLoopActivityCount { get; set; }
}