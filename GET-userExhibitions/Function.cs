using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Lambda.APIGatewayEvents;
using Amazon.Lambda.Core;
using Newtonsoft.Json;

// This attribute is essential for Lambda to find your function handler
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace GET_userExhibitions
{
    public class Function
    {
        public async Task<APIGatewayHttpApiV2ProxyResponse> FunctionHandler(APIGatewayHttpApiV2ProxyRequest request, ILambdaContext context)
        {
            var log = context.Logger;
            log.LogInformation($"Request received: {JsonConvert.SerializeObject(request, Formatting.Indented)}");

            try
            {
                if (!request.QueryStringParameters.TryGetValue("userID", out string userID) || string.IsNullOrEmpty(userID))
                {
                    log.LogError("userID is missing or empty in the query parameters");
                    return new APIGatewayHttpApiV2ProxyResponse
                    {
                        StatusCode = 400,
                        Body = JsonConvert.SerializeObject(new { message = "userID is missing or empty in the query parameters." })
                    };
                }

                log.LogInformation($"Querying tables for userID: {userID}");

                string publicTableName = "PublicExhibitions";
                string privateTableName = "PrivateExhibitions";

                List<Dictionary<string, AttributeValue>> results;
                try
                {
                    results = await QueryTables(userID, publicTableName, privateTableName);
                    log.LogInformation($"Query completed. Number of results: {results.Count}");
                }
                catch (Exception ex)
                {
                    log.LogError($"Error in QueryTables: {ex.Message}");
                    log.LogError($"Stack Trace: {ex.StackTrace}");
                    return new APIGatewayHttpApiV2ProxyResponse
                    {
                        StatusCode = 500,
                        Body = JsonConvert.SerializeObject(new { message = "An error occurred while querying the database.", error = ex.Message })
                    };
                }

                var transformedResults = results.Select(item => new
                {
                    ExhibitionID = item.GetValueOrDefault("ExhibitionID")?.N,
                    ExhibitionName = item.GetValueOrDefault("ExhibitionName")?.S,
                    ExhibitionLength = item.GetValueOrDefault("ExhibitionLength")?.N,
                    ExhibitionImage = item.GetValueOrDefault("ExhibitionImage")?.S,
                    ExhibitionPublic = item.GetValueOrDefault("ExhibitionPublic")?.N ?? item.GetValueOrDefault("ExhibitionPublic")?.S,
                    ExhibitContent = item.GetValueOrDefault("ExhibitContent")?.L?.Select(content => new
                    {
                        CreationDate = content.M?.GetValueOrDefault("CreationDate")?.N,
                        ItemClassification = content.M?.GetValueOrDefault("ItemClassification")?.S,
                        ItemObjectLink = content.M?.GetValueOrDefault("ItemObjectLink")?.S,
                        ItemDepartment = content.M?.GetValueOrDefault("ItemDepartment")?.S,
                        ItemTitle = content.M?.GetValueOrDefault("ItemTitle")?.S,
                        ArtistBirthplace = content.M?.GetValueOrDefault("ArtistBirthplace")?.S,
                        ArtistName = content.M?.GetValueOrDefault("ArtistName")?.S,
                        ItemTechnique = content.M?.GetValueOrDefault("ItemTechnique")?.S,
                        ItemCentury = content.M?.GetValueOrDefault("ItemCentury")?.S,
                        ItemCreditline = content.M?.GetValueOrDefault("ItemCreditline")?.S,
                        ItemID = content.M?.GetValueOrDefault("ItemID")?.N

                    }).ToList()
                }).ToList();

                return new APIGatewayHttpApiV2ProxyResponse
                {
                    StatusCode = 200,
                    Body = JsonConvert.SerializeObject(new { exhibitions = transformedResults })
                };
            }
            catch (Exception ex)
            {
                log.LogError($"Unexpected error in FunctionHandler: {ex.Message}");
                log.LogError($"Stack Trace: {ex.StackTrace}");
                return new APIGatewayHttpApiV2ProxyResponse
                {
                    StatusCode = 500,
                    Body = JsonConvert.SerializeObject(new { message = "An unexpected error occurred.", error = ex.Message })
                };
            }
        }

        private async Task<List<Dictionary<string, AttributeValue>>> QueryTables(string userId, string publicTableName, string privateTableName)
        {
            var dynamoDbClient = new AmazonDynamoDBClient();

            try
            {
                var publicTableTask = QueryTable(dynamoDbClient, publicTableName, userId);
                var privateTableTask = QueryTable(dynamoDbClient, privateTableName, userId);

                await Task.WhenAll(publicTableTask, privateTableTask);

                var combinedResults = new List<Dictionary<string, AttributeValue>>();
                combinedResults.AddRange(publicTableTask.Result);
                combinedResults.AddRange(privateTableTask.Result);

                return combinedResults;
            }
            catch (Exception ex)
            {
                throw new Exception($"Error in QueryTables: {ex.Message}", ex);
            }
        }

        private async Task<List<Dictionary<string, AttributeValue>>> QueryTable(IAmazonDynamoDB dynamoDbClient, string tableName, string userId)
        {
            try
            {
                var queryRequest = new QueryRequest
                {
                    TableName = tableName,
                    KeyConditionExpression = "PK = :userId",
                    ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                    {
                        {":userId", new AttributeValue { S = userId }}
                    },
                    ScanIndexForward = false
                };

                var queryResponse = await dynamoDbClient.QueryAsync(queryRequest);
                return queryResponse.Items;
            }
            catch (Exception ex)
            {
                throw new Exception($"Error querying table {tableName}: {ex.Message}", ex);
            }
        }
    }
}