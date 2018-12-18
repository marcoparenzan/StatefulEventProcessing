using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Web;

using static System.Console;

namespace StatefulEventProcessor.Services
{
    public class RedisStatefulEventService: IHostedService
    {
        private IConfiguration configuration;
        private IHubContext<Hubs.H0> hubContext;

        public RedisStatefulEventService(IConfiguration configuration, IHubContext<Hubs.H0> hubContext)
        {
            this.configuration = configuration;
            this.hubContext = hubContext;
        }

        Task IHostedService.StartAsync(CancellationToken cancellationToken)
        {
            return Start();
        }

        Task IHostedService.StopAsync(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }

        private async Task Notify(string from, string message)
        {
            await hubContext.Clients.All.SendAsync("update", message, from);
        }

        // https://github.com/Azure/amqpnetlite/tree/master/Examples/ServiceBus/Scenarios
        public async Task Start()
        {
            var eventHubEndpointAddress = configuration["eventHubEndpointAddress"];
            var eventHubEndpointPort = int.Parse(configuration["eventHubEndpointPort"]);
            var eventHubSharedAccessKeyName = configuration["eventHubSharedAccessKeyName"];
            var eventHubSharedAccessKey = configuration["eventHubSharedAccessKey"];

            var eventHubEntityPath = configuration["eventHubEntityPath"];
            var eventHubConsumerGroup = configuration["eventHubConsumerGroup"];
            var eventHubPartitionId = configuration["eventHubPartitionId"];

            var storageConnectionString = configuration["storageConnectionString"];
            var storagePendingTimeWindowsQueueName = configuration["storagePendingTimeWindowsQueue"];

            var format = "ses1_0";
            var sessionName = $"{configuration["sessionName"]}-{eventHubPartitionId}";

            // storage init

            var storageAccount = CloudStorageAccount.Parse(storageConnectionString);
            var queueClient = storageAccount.CreateCloudQueueClient();
            var storagePendingTimeWindowsQueue = queueClient.GetQueueReference($"{storagePendingTimeWindowsQueueName}-p{eventHubPartitionId}");
            await storagePendingTimeWindowsQueue.CreateIfNotExistsAsync();

            // redis init

            var redisConnectionString = configuration["redisConnectionString"];
            var redis = ConnectionMultiplexer.Connect($"{redisConnectionString}");
            var redisDb = redis.GetDatabase();

            // redis based configuration
            var redisStartTimeKey = (RedisKey)$"{format}/{sessionName}/startTime";
            var startTime = DateTime.Parse("2018-01-01T00:00");
            if (redisDb.KeyExists(redisStartTimeKey))
            {
                startTime = DateTime.Parse(redisDb.StringGet(redisStartTimeKey));
            }

            var redisOffsetKey = (RedisKey)$"{format}/{sessionName}/offset";
            string initialOffset = null;
            if (redisDb.KeyExists(redisOffsetKey))
            {
                initialOffset = redisDb.StringGet(redisOffsetKey);
            }

            var redisDataWindowTimeoutKey = (RedisKey)$"{format}/{sessionName}/dataWindowTimeout";
            var redisDataWindowTimeout = TimeSpan.FromSeconds(30); // 60 * 20);
            if (redisDb.KeyExists(redisDataWindowTimeoutKey))
            {
                redisDataWindowTimeout = TimeSpan.Parse(redisDb.StringGet(redisDataWindowTimeoutKey));
            }

            // initial sync

            // dataWindows
            var hoppingWindow15 =
                HoppingWindow.New(TimeSpan.FromSeconds(10))
                .From(startTime);

            WriteLine($"Started at {DateTime.Now}");

            // QueueWriter

#pragma warning disable CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
            Task.Run(() => QueueWriter(configuration));
#pragma warning restore CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed

            // subs
            var subscriber = redis.GetSubscriber();
            subscriber.Subscribe($"__key*@{redisDb.Database}__:*", (Action<RedisChannel, RedisValue>)(async (channel, key) =>
            {
                var k = key.ToString();
                switch (k)
                {
                    case "expired":
                        var c = channel.ToString();
                        c = c.Substring(c.IndexOf(":") + 1);

                        var qs = HttpUtility.ParseQueryString(c.Substring(c.IndexOf("?") + 1));
                        var keyStore = (RedisKey) qs["keyStore"];
                        var values = redisDb.ListRange(keyStore, 1);
                        if (values.Length > 0)
                        {
                            var json = JsonConvert.SerializeObject(new {
                                type = $"{format}/aggregates/hoppingWindow15",
                                key = keyStore.ToString(),
                                values = values
                            });
                            var queueMessage = new CloudQueueMessage(json);
                            await storagePendingTimeWindowsQueue.AddMessageAsync(queueMessage);
                            await Notify("redisexpire", json);
                        }
                        redisDb.KeyDelete(keyStore);
                        WriteLine($"Expired: {keyStore}");
                        break;
                    default:
                        //WriteLine($">>>> {v} | {channel}");
                        break;
                }
            }));

            // let's go
            using (var node = new EventHubAMQPNode(eventHubEndpointAddress, eventHubEndpointPort, eventHubSharedAccessKeyName, eventHubSharedAccessKey))
            {
                node.HandleJson(eventHubEntityPath, eventHubPartitionId, eventHubConsumerGroup, initialOffset: initialOffset, handler:  (sequenceNumber, offset, enqueuedTime, json) =>
                {
                    var deviceTime = json.Value<DateTime>("deviceTime");
                    var deviceId = json.Value<string>("deviceId");
                    var propertyId = json.Value<string>("propertyId");
                    var propertyValue = json.Value<double>("propertyValue");

                    var path = $"{deviceId}/{propertyId}";

                    var result = redisDb.StringSet(redisOffsetKey, offset);
                    WriteLine($"offset={offset}");

                    hoppingWindow15.At(deviceTime, (time, hop) =>
                    {
                        var keyStoreText = $"{format}/{sessionName}/store/hoppingWindow15/{time.ToString("yyyy-MM-ddTHH:mm")}Z/{path}";
                        var keyStore = (RedisKey)keyStoreText;
                        if (!redisDb.KeyExists(keyStore))
                        {
                            var id = Guid.NewGuid();
                            redisDb.ListRightPush(keyStore, $"{id}");
                            var keySchedule = (RedisKey)$"{format}/{sessionName}/schedule/hoppingWindow15/{id}?keyStore={keyStoreText}&created={DateTime.UtcNow.ToString("u")}";
                            redisDb.StringSet(keySchedule, string.Empty);
                            redisDb.KeyExpire(keySchedule, redisDataWindowTimeout);
                        }
                        var msg = $"o={offset}&s={sequenceNumber}&v={propertyValue}&t={deviceTime}";
                        redisDb.ListRightPush(keyStore, msg);
                        Notify("hoppingWindow15", $"{keyStore}={msg}").Wait();
                        return Task.CompletedTask;
                    });

                });

                await Task.Delay(Timeout.Infinite);
            }
        }

        private async Task QueueWriter(IConfiguration configuration)
        {
            var storageConnectionString = configuration["storageConnectionString"];
            //var storageBlobTimeWindowsContainerName = configuration["storageBlobTimeWindowsContainer"];
            var storagePendingTimeWindowsQueueName = configuration["storagePendingTimeWindowsQueue"];

            var eventHubPartitionId = configuration["eventHubPartitionId"];

            var sqlConnectionString = configuration["sqlConnectionString"];

            // storage init

            var storageAccount = CloudStorageAccount.Parse(storageConnectionString);

            var blobClient = storageAccount.CreateCloudBlobClient();

            var queueClient = storageAccount.CreateCloudQueueClient();
            var storagePendingTimeWindowsQueue = queueClient.GetQueueReference($"{storagePendingTimeWindowsQueueName}-p{eventHubPartitionId}");
            await storagePendingTimeWindowsQueue.CreateIfNotExistsAsync();

            using (var conn = new SqlConnection(sqlConnectionString))
            {
                var quarterHourAggregate = conn.CreateCommand();
                quarterHourAggregate.CommandText = "INSERT INTO DevicePropertyStats (TimeFrame, TimeStamp, DeviceId, PropertyId, Value, ValueType) VALUES (@TimeFrame, @TimeStamp, @DeviceId, @PropertyId, @Value, @ValueType)";
                var timeFrameP = quarterHourAggregate.Parameters.Add("@timeFrame", SqlDbType.Int);
                var timeStampP = quarterHourAggregate.Parameters.Add("@timeStamp", SqlDbType.DateTime);
                var deviceIdP = quarterHourAggregate.Parameters.Add("@deviceId", SqlDbType.NVarChar, 8);
                var propertyIdP = quarterHourAggregate.Parameters.Add("@propertyId", SqlDbType.NVarChar, 8);
                var valueP = quarterHourAggregate.Parameters.Add("@value", SqlDbType.Decimal);
                valueP.Scale = 3;
                valueP.Precision = 18;
                var valueTypeP = quarterHourAggregate.Parameters.Add("@valueType", SqlDbType.Int);

                while (true)
                {
                    var loop = 0;
                    while (true)
                    {
                        var message = await storagePendingTimeWindowsQueue.GetMessageAsync();
                        if (message == null)
                        {
                            loop++;
                            if (loop == 3)
                            {
                                break;
                            }
                            continue;
                        }

                        try
                        {
                            var json = JsonConvert.DeserializeObject<JObject>(message.AsString);

                            var type = json.Value<string>("type");
                            if (type.EndsWith("aggregates/hoppingWindow15"))
                            {
                                var key = json.Value<string>("key");
                                var values = json.Value<JArray>("values");
                                var keyParts = key.Split("/");

                                var deviceId = int.Parse(keyParts[keyParts.Length - 2]);
                                var propertyId = int.Parse(keyParts[keyParts.Length - 1]);

                                var vs = new List<(decimal v, DateTime t)>();

                                foreach (var value in values)
                                {
                                    var qs = HttpUtility.ParseQueryString(value.Value<string>());
                                    var offset = qs["o"];
                                    var sequenceNumber = qs["s"];
                                    var v = decimal.Parse(qs["v"]);
                                    var t = DateTime.Parse(qs["t"]);
                                    vs.Add((v, t));
                                }

                                if (conn.State != System.Data.ConnectionState.Open)
                                {
                                    await conn.OpenAsync();
                                }

                                timeFrameP.Value = TimeFrame.QuarterHour;
                                timeStampP.Value = vs.Max(xx => xx.t);
                                deviceIdP.Value = deviceId;
                                propertyIdP.Value = propertyId;
                                valueP.Value = vs.Average(xx => xx.v);
                                valueTypeP.Value = ValueType.Average;

                                quarterHourAggregate.ExecuteNonQuery();
                                Notify("sql", $"{deviceId},{propertyId} @{timeStampP.Value}").Wait();
                            }
                        }
                        catch (Exception ex)
                        {
                        }

                        await storagePendingTimeWindowsQueue.DeleteMessageAsync(message);
                    }
                    if (conn.State == System.Data.ConnectionState.Open)
                    {
                        conn.Close();
                        await Task.Delay(10000);
                    }
                }
            }
        }
    }
}
