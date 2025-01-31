using System;
using System.Collections.Generic;
using System.Text.Json.Serialization;

public class M365User
{

    public string UserPrincipalName { get; set; }
    public bool HasCopilotLicense { get; set; }
}