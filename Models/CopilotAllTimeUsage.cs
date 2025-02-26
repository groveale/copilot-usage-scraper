public class AllTimeUsage
{
    public string UPN { get; set; }
    // CopilotAllUp / CopilotChat / Teams / Outlook / Word / Excel / PowerPoint / OneNote / Loop
    public AppType App {get; set; }
    public string DisplayName { get; set; }
    // Copilot all up
    public int DailyAllTimeActivityCount { get; set; }
    public int BestDailyStreak { get; set; }
    public int CurrentDailyStreak { get; set; }

    public string GetAppString()
    {
        return App.ToString();
    }
}

public enum AppType
{
    CopilotAllUp,
    CopilotChat,
    Teams,
    Outlook,
    Word,
    Excel,
    PowerPoint,
    OneNote,
    Loop
}

