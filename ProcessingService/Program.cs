using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace ProcessingService;

class Program
{
    static void Main(string[] args)
    {
        var factory = new ConnectionFactory { HostName = "localhost" };
        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();

        channel.QueueDeclare(queue: "DataCaptureQueue",
                             durable: false,
                             exclusive: false,
                             autoDelete: false,
                             arguments: null);

        Console.WriteLine(" [*] Waiting for messages.");

        var consumer = new EventingBasicConsumer(channel);
        Dictionary<int, FileStream> fileStreams = new Dictionary<int, FileStream>();

        consumer.Received += (model, ea) =>
        {
            try
            {
                System.Console.WriteLine("Message received");
                var body = ea.Body.ToArray();
                var headers = ea.BasicProperties.Headers;
                string filePath = Encoding.UTF8.GetString((byte[])headers["filePath"]);
                int chunkID = headers["chunkID"] is byte[] chunkIdBytes ? BitConverter.ToInt32(chunkIdBytes, 0) : (int)headers["chunkID"];
                int totalChunks = headers["totalChunks"] is byte[] totalChunksBytes ? BitConverter.ToInt32(totalChunksBytes, 0) : (int)headers["totalChunks"];
                int fileKey = filePath.GetHashCode();

                if (!fileStreams.ContainsKey(fileKey))
                {
                    string tempFilePath = Path.Combine("/Users/Umid_Rahimov/Documents/LocalFolder/", Path.GetFileName(filePath) + ".tmp");
                    fileStreams[fileKey] = new FileStream(tempFilePath, FileMode.Create, FileAccess.Write, FileShare.None);
                }
                FileStream fs = fileStreams[fileKey];
                fs.Write(body, 0, body.Length);

                if (chunkID == totalChunks)
                {
                    string finalPath = fs.Name.Replace(".tmp", "");
                    fs.Close();
                    File.Move(fs.Name, finalPath, true);
                    fileStreams.Remove(fileKey);
                    Console.WriteLine($"File reconstructed and saved to {finalPath}");
                }
                channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error processing message: {ex.Message}");
                channel.BasicNack(deliveryTag: ea.DeliveryTag, multiple: false, requeue: true);
            }
        };

        channel.BasicConsume(queue: "DataCaptureQueue",
                             autoAck: false,
                             consumer: consumer);

        Console.WriteLine(" Press [enter] to exit.");

        Console.ReadLine();
    }
}
