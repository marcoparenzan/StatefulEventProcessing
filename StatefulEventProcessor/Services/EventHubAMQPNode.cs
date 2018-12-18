using Amqp;
using Amqp.Framing;
using Amqp.Types;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Text;

namespace StatefulEventProcessor.Services
{
    public class EventHubAMQPNode: IDisposable
    {
        public Connection Connection { get; private set; }
        public Session Session { get; private set; }

        public EventHubAMQPNode(string eventHubEndpointAddress, int eventHubEndpointPort, string eventHubSharedAccessKeyName, string eventHubSharedAccessKey)
        {
            var username = eventHubSharedAccessKeyName;
            var password = eventHubSharedAccessKey;

            //var factory = new WebSocketTransportFactory("AMQPWSB10");
            //var address = new Address(eventHubEndpointAddress, 443, username, password, "/$servicebus/websocket", "wss");
            var factory = new ConnectionFactory();
            var address = new Address(eventHubEndpointAddress, eventHubEndpointPort, username, password);
            Connection = factory.CreateAsync(address).Result;
            Session = new Session(Connection);
        }

        public EventHubAMQPNode Handle(string entityPath, string partitionId, string consumerGroup, Action<Message> handler, string offset = null)
        {
            var partitionAddress = $"{entityPath}/ConsumerGroups/{consumerGroup}/Partitions/{partitionId}";
            var receiverLink = CreateReceiverLink(Session, partitionAddress, name: $"receiver-{partitionId}", offset: offset);

            while (true)
            {
                var message = receiverLink.Receive();
                if (message == null)
                {
                    continue;
                }

                handler(message);

                receiverLink.Accept(message);
            }

            //receiverLink.Close();
        }

        public EventHubAMQPNode HandleJson(string entityPath, string partitionId, string consumerGroup, Action<long, string, DateTime, JObject> handler, string initialOffset = null)
        {
            return Handle(entityPath, partitionId, consumerGroup, message =>
            {
                var sequenceNumber = (long)message.MessageAnnotations[new Symbol("x-opt-sequence-number")];
                var offset = (string)message.MessageAnnotations[new Symbol("x-opt-offset")];
                var enqueuedTime = (DateTime)message.MessageAnnotations[new Symbol("x-opt-enqueued-time")];
                var bytes = (byte[])message.Body;
                var json = JsonConvert.DeserializeObject<JObject>(Encoding.UTF8.GetString(bytes));

                handler(sequenceNumber, offset, enqueuedTime, json);
            }, initialOffset);
        }

        public string[] GetPartitions(Session session, string entity)
        {
            var targetNode = $"{Guid.NewGuid()}";

            var sender = CreateSenderLink(session, "$management");
            var receiver = CreateReceiverLink(session, "$management");

            var request = new Message();
            request.Properties = new Properties() { MessageId = $"{Guid.NewGuid()}", ReplyTo = targetNode };
            request.ApplicationProperties = new ApplicationProperties();
            request.ApplicationProperties["operation"] = "READ";
            request.ApplicationProperties["name"] = entity;
            request.ApplicationProperties["type"] = "com.microsoft:eventhub";
            sender.Send(request, null, null);

            var response = receiver.Receive();
            if (response == null)
            {
                throw new Exception("No response was received.");
            }

            receiver.Accept(response);
            receiver.Close();
            sender.Close();

            var partitions = (string[])((Map)response.Body)["partition_ids"];

            return partitions;
        }

        public SenderLink CreateSenderLink(Session session, string address)
        {
            var attach = CreateAttach(target: address);
            var link = new SenderLink(session, attach.LinkName, attach, (l, a) =>
            {
            });
            return link;
        }

        public ReceiverLink CreateReceiverLink(Session session, string address, string name = null, string offset = null)
        {
            Map filters = null;
            if (!string.IsNullOrWhiteSpace(offset))
            {
                filters = new Map();
                filters.Add(new Symbol("apache.org:selector-filter:string"),
                    new DescribedValue(new Symbol("apache.org:selector-filter:string"), $"amqp.annotation.x-opt-offset >= '{offset}'"));
            }

            var attach = CreateAttach(source: address, name: name, filters: filters);
            var link = new ReceiverLink(session, name ?? attach.LinkName, attach, (l, a) =>
            {
            });
            return link;
        }

        public Attach CreateAttach(string source = null, string target = null, Map filters = null, string name = null)
        {
            var attach = new Attach
            {
                Role = false,
                InitialDeliveryCount = 0,
                LinkName = name ?? Guid.NewGuid().ToString("N"),
                SndSettleMode = SenderSettleMode.Unsettled,
                RcvSettleMode = ReceiverSettleMode.First,
                Properties = new Amqp.Types.Fields()
            };
            if (!string.IsNullOrWhiteSpace(source)) attach.Source = new Source { Address = source, FilterSet = filters };
            if (!string.IsNullOrWhiteSpace(target)) attach.Target = new Target { Address = target };
            //attach.Properties.Add(new Symbol("com.microsoft:timeout"), "86400");
            //attach.Properties.Add(new Symbol("com.microsoft:client-version"), "99");
            //attach.Properties.Add(new Symbol("com.microsoft:api-version"), "2018-06-30");
            //attach.Properties.Add(new Symbol("com.microsoft:channel-correlation-id"), channelCorrelationId);
            return attach;
        }

        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    // TODO: dispose managed state (managed objects).
                }

                // TODO: free unmanaged resources (unmanaged objects) and override a finalizer below.
                // TODO: set large fields to null.

                Session.CloseAsync().Wait();
                Connection.CloseAsync().Wait();

                disposedValue = true;
            }
        }

        // TODO: override a finalizer only if Dispose(bool disposing) above has code to free unmanaged resources.
        // ~EventHubAMQPNode() {
        //   // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
        //   Dispose(false);
        // }

        // This code added to correctly implement the disposable pattern.
        void IDisposable.Dispose()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            Dispose(true);
            // TODO: uncomment the following line if the finalizer is overridden above.
            // GC.SuppressFinalize(this);
        }
        #endregion
    }
}
