using Confluent.Kafka;

namespace Consumer.Net
{
    internal class Program
    {
        static void Main(string[] args)
        {
            const string topic = "test-topic";

            var config = new ConsumerConfig
            {
                BootstrapServers = "127.0.0.1:19094, 127.0.0.1:29094, 127.0.0.1:39094",
                SecurityProtocol = SecurityProtocol.SaslPlaintext,
                SaslMechanism = SaslMechanism.Plain,
                SaslUsername = "local_kafka_user",
                SaslPassword = "local_pass",

                GroupId = $"new_group",
                GroupInstanceId = $"CONSUMER-{Guid.NewGuid()}",
                
                
                SessionTimeoutMs = 10000, // Зесь я изменил таймаут, поскольку в реализации Confluent.Kafka.Net нельзя SessionTimeoutMs делать большим, чем MaxPollIntervalMs
                HeartbeatIntervalMs = 5000,
                MaxPollIntervalMs = 10000,

                AutoOffsetReset = AutoOffsetReset.Latest,

                EnableAutoCommit = false,

                AllowAutoCreateTopics = false,

                MaxPartitionFetchBytes = 81920
            };

            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) => {
                e.Cancel = true; // prevent the process from terminating.
                cts.Cancel();
            };

            using (var consumer = new ConsumerBuilder<string, string>(config).SetKeyDeserializer(Deserializers.Utf8).SetValueDeserializer(Deserializers.Utf8).Build())
            {
                consumer.Subscribe(topic);
                int cnt = 0;
                try
                {
                    while (true)
                    {
                        // Здесь я указываю, сколько времени консьюмер будет ожидать новых сообщений
                        cts.CancelAfter(TimeSpan.FromSeconds(100));
                        var cr = consumer.Consume(cts.Token);
                        Console.WriteLine(cr.Message.Value);
                        
                        cnt++;
                    }
                }
                catch (OperationCanceledException)
                {
                    // Ctrl-C was pressed or timeout expired.
                    if (cnt > 0)
                    {
                        Console.WriteLine($"Вычитано {cnt} сообщений");
                        Console.WriteLine("Выполнение commit offset'а в Kafka");
                        consumer.Commit();
                        Console.WriteLine("Сommit offset'а выполнен успешно.");
                    }
                    else
                    {
                        Console.WriteLine("В топике Kafka нет новых сообщений для текущей консьюмер-группы");
                    }
                }
                finally
                {
                    consumer.Close();
                }
            }
        }
    }
}
