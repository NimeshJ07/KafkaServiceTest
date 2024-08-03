using Confluent.Kafka;
using Newtonsoft.Json;
using System;
using System.Threading.Tasks;

class DemoProducer
{
    public static async Task Main(string[] args)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = "localhost:9092",
        };

        using var producer = new ProducerBuilder<Null, string>(config).Build();

        try
        {
            string? name;
            while ((name = Console.ReadLine()) != null)
            {
                var message = new Message<Null, string>
                {
                    Value = JsonConvert.SerializeObject(new Demo(name, 22))
                };

                var res = await producer.ProduceAsync("test-topic", message);

                Console.WriteLine($"Delivered '{res.Value}' to '{res.TopicPartitionOffset}'");
            }
        }
        catch (ProduceException<Null, string> ex)
        {
            Console.WriteLine($"Delivery failed: {ex.Error.Reason}");
        }
    }
}

public record Demo(string Name, int Age);
