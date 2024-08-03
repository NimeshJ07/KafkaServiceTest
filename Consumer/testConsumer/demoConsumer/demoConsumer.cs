using Confluent.Kafka;
using Newtonsoft.Json;

var config = new ConsumerConfig
{
    GroupId = "test-consumer-group",
    BootstrapServers = "localhost:9092",
    AutoOffsetReset = AutoOffsetReset.Earliest,
};

var consumer = new ConsumerBuilder<Null,string>(config).Build();

consumer.Subscribe("test-topic");

CancellationTokenSource cts = new CancellationTokenSource();

try
{
    while (true)
    {
        var res = consumer.Consume(
            cts.Token    
        );
        if(res.Message != null)
        {
            var demos = JsonConvert.DeserializeObject<demo>(
                res.Message.Value
            );
            Console.WriteLine($"Name : {demos.name} , Age : {demos.age}");
        }
    }
}
catch (Exception)
{
	throw;
}

public record demo(string name, int age);