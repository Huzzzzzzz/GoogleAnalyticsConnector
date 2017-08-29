                                                    
//  Title: Google Analytics Connector for Power BI  
//  Created by: Ruth Pozuelo Martinez (@curbalen) -https://curbal.com
//  Based on:  Miguel Escobar (@EscobarMiguel90) code for youtube Analytics connector -https://github.com/migueesc123/YoutubeAnalytics


section GAAnalytics;

appKey="enteryourappkey";
appSecret="enteryourappsecret";
redirectUrl = "https://oauth.powerbi.com/views/oauthredirect.html";
token_uri = "https://accounts.google.com/o/oauth2/token";
authorize_uri = "https://accounts.google.com/o/oauth2/auth";
logout_uri = "https://accounts.google.com/logout";

windowWidth = 720;
windowHeight = 1024;

// See https://developers.google.com/identity/protocols/googlescopes for scopes 
scope_prefix = "https://www.googleapis.com/auth/";
scopes = {
"analytics.readonly"
};

Value.IfNull = (a, b) => if a <> null then a else b;

GetScopeString = (scopes as list, optional scopePrefix as text) as text =>
    let
        prefix = Value.IfNull(scopePrefix, ""),
        addPrefix = List.Transform(scopes, each prefix & _),
        asText = Text.Combine(addPrefix, " ")
    in
        asText;

[DataSource.Kind="GAAnalytics", Publish="GAAnalytics.Publish"]

shared NavigationTable.Simple = () =>
    let
        objects = #table(
            {"Name",       "Key",        "Data",                           "ItemKind", "ItemName", "IsLeaf"},{
            {"GET Reports", "Googleanalytics.GETReports", Googleanalytics.GETReports,       "Function", "Function", true}
            }),
        NavTable = Table.ToNavigationTable(objects, {"Key"}, "Name", "Data", "ItemKind", "ItemName", "IsLeaf")
    in
        NavTable;
 
 [DataSource.Kind="GAAnalytics"]
 shared Googleanalytics.GETReports =   Value.ReplaceType(GetReports,GetReportsType);
 

GetReportsType = type function (
    optional PStartDate as (type date meta [ 
        Documentation.FieldCaption = "Start Date"
    ]),
     optional PEndDate as (type date meta [ 
        Documentation.FieldCaption = "End Date"
    ]),
     optional Pids as (type text meta [
        Documentation.FieldCaption = "Google account id",
        Documentation.FieldDescription = "9-letter account id",
        Documentation.SampleValues = {"ga:88362167"}
    ]),
     optional Pmetrics as (type text meta [
        Documentation.FieldCaption = "Metrics",
        Documentation.SampleValues = {"ga:sessions"}
    ]),
      optional Pdimensions as (type text meta [
        Documentation.FieldCaption = "Dimensions",
        Documentation.SampleValues = {"ga:date"}
    ]),
      optional Psort as (type text meta [
        Documentation.FieldCaption = "Sort",
        Documentation.SampleValues = {"ga:country,ga:browser"}
    ]),
      optional Pfilters as (type text meta [
        Documentation.FieldCaption = "Filters",
        Documentation.SampleValues = {"ga:medium"}
    ]),
      optional Psegment as (type text meta [
        Documentation.FieldCaption = "Max Results"
    ]))

    as table meta [
        Documentation.Name = "Google Analytics API v4",
        Documentation.LongDescription = "Read the full Google Analytics API documentation from Google and find how to construct your query at:https://ga-dev-tools.appspot.com/query-explorer/",
        Documentation.Examples = {[
            Description = "Quick query to get the views by day from June 1st, 2017 until June 3rd, 2017",
            Code = "Check the Documentation at: https://ga-dev-tools.appspot.com/query-explorer/
            
            Example: 
            GoogleAnalytics.GETReports (
             #date(2017,6,1), 
             #date(2017,6,3), 
             ""ga:88362167"", 
             ""ga:sessions"", 
             ""ga:date"", 
             null, 
             null, 
             null, 
             null)",
                        Result = "#table({""date"",""sessions""},{
                        {""2017-06-01"",""230""},
                        {""2017-06-02"",""255""},
                        {""2017-06-03"",""386""}}))"
        ]}
    ];

  
    GetReports =  (
 optional PStartDate as date, 
 optional PEndDate as date,
 optional Pids as text,
 optional Pmetrics as text, 
 optional Pdimensions as text,
 optional Psort as text, 
 optional Pfilters as text, 
 optional Psegment as text) as table =>
     let
       Start = Date.ToText ( PStartDate, "yyyy-MM-dd"),
      End =  Date.ToText ( PEndDate, "yyyy-MM-dd"),
      requesturl = "https://www.googleapis.com/analytics/v3/data/ga",
    GETInfo= [ 
        ids = Pids,
        #"start-date" = Start,
        #"end-date" = End,
        metrics = Pmetrics,
        dimensions=Pdimensions,
        filters= Pfilters,
        sort = Psort,
        segment= Psegment],
    CleanedRecord = Record.RemoveFields( GETInfo,  Table.SelectRows( Record.ToTable ( GETInfo), each ([Value] = null))[Name]),
    Request= 
        Json.Document(
                     Web.Contents( requesturl, [RelativePath = "?" & Uri.BuildQueryString(CleanedRecord)] ) ),
    ColumnHeaders = Table.FromRecords( Request[columnHeaders] )[name],
    DataValues =  List.Transform( Request[rows], each Record.FromList( _, ColumnHeaders)),
    OutputTable = Table.FromRecords ( DataValues)
        in 
            OutputTable;




// Data Source Kind description
GAAnalytics = [
    Authentication = [
        OAuth = [
            StartLogin = StartLogin,
            FinishLogin = FinishLogin
        ]
    ],
    Label = Extension.LoadString("DataSourceLabel")
];

// Data Source UI publishing description
GAAnalytics.Publish = [
    Beta = true,
    Category = "Other",
    ButtonText = { Extension.LoadString("ButtonTitle"), Extension.LoadString("ButtonHelp") },
    LearnMoreUrl = "https://powerbi.microsoft.com/",
    SourceImage = YoutubeAnalytics.Icons,
    SourceTypeImage = YoutubeAnalytics.Icons
];

YoutubeAnalytics.Icons = [
    Icon16 = { Extension.Contents("PQExtension116.png"), Extension.Contents("PQExtension120.png"), Extension.Contents("PQExtension124.png"), Extension.Contents("PQExtension132.png") },
    Icon32 = { Extension.Contents("PQExtension132.png"), Extension.Contents("PQExtension140.png"), Extension.Contents("PQExtension148.png"), Extension.Contents("PQExtension164.png") }
];

StartLogin = (resourceUrl, state, display) =>
    let
        AuthorizeUrl = authorize_uri & "?" & Uri.BuildQueryString([
        client_id = appKey,  
        redirect_uri = redirectUrl,   
        state="security_token",
        scope = GetScopeString(scopes, scope_prefix),
        response_type = "code",
        response_mode = "query",
        access_type="offline",
        login = "login"    
    ])
    in
        [
            LoginUri = AuthorizeUrl,
            CallbackUri = redirectUrl,
            WindowHeight = 720,
            WindowWidth = 1024,
            Context = null
        ];

FinishLogin = (context, callbackUri, state) =>
    let
        parts = Uri.Parts(callbackUri)[Query],
        result = if (Record.HasFields(parts, {"error", "error_description"})) then 
                    error Error.Record(parts[error], parts[error_description], parts)
                 else
                   TokenMethod(parts[code])
    in
        result;

TokenMethod = (code) =>
    let
        response = Web.Contents(token_uri, [
            Content = Text.ToBinary(Uri.BuildQueryString([
                grant_type = "authorization_code",
                client_id = appKey,
                client_secret = appSecret,
                code = code,
                redirect_uri = redirectUrl])),
            Headers=[#"Content-type" = "application/x-www-form-urlencoded",#"Accept" = "application/json"], ManualStatusHandling = {400}]),
        body = Json.Document(response),
        result = if (Record.HasFields(body, {"error", "error_description"})) then 
                    error Error.Record(body[error], body[error_description], body)
                 else
                    body
    in
        result;

Table.ToNavigationTable = (
    table as table,
    keyColumns as list,
    nameColumn as text,
    dataColumn as text,
    itemKindColumn as text,
    itemNameColumn as text,
    isLeafColumn as text
) as table =>
    let
        tableType = Value.Type(table),
        newTableType = Type.AddTableKey(tableType, keyColumns, true) meta 
        [
            NavigationTable.NameColumn = nameColumn, 
            NavigationTable.DataColumn = dataColumn,
            NavigationTable.ItemKindColumn = itemKindColumn, 
            Preview.DelayColumn = itemNameColumn, 
            NavigationTable.IsLeafColumn = isLeafColumn
        ],
        navigationTable = Value.ReplaceType(table, newTableType)
    in
        navigationTable;

