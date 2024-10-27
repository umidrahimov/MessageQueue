using System.Text;
using RabbitMQ.Client;

namespace DataCaptureService;

class Program
{
    static async Task Main(string[] args)
    {
        using var watcher = new FileSystemWatcher(@"/Users/Umid_Rahimov/Documents/SharedFolder");

        watcher.NotifyFilter = NotifyFilters.LastWrite | NotifyFilters.FileName | NotifyFilters.DirectoryName;

        watcher.Created += OnCreated;
        watcher.Error += OnError;

        //.flag extention will trigger file queuing
        watcher.Filter = "*.flag";
        watcher.IncludeSubdirectories = true;
        watcher.EnableRaisingEvents = true;

        Console.WriteLine("Monitoring started. Type 'Q' or 'Quit' to exit.");
        await MonitorUntilQuitAsync();
        Console.WriteLine("Monitoring stopped.");
    }
    private static async Task MonitorUntilQuitAsync()
    {
        while (true)
        {
            var input = await Task.Run(() => Console.ReadLine());
            if (string.Equals(input, "Q", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(input, "Quit", StringComparison.OrdinalIgnoreCase))
            {
                break;
            }
        }
    }
    private static void OnCreated(object sender, FileSystemEventArgs e)
    {
        string filePath = e.FullPath.Replace(".flag", "");
        if (File.Exists(filePath))
        {
            SendMessageToQueue(filePath);
        }
        else
        {
            Console.WriteLine($"File not found: {filePath}");
        }

        Console.WriteLine(filePath);
    }

    private static void OnError(object sender, ErrorEventArgs e) =>
    PrintException(e.GetException());

    private static void PrintException(Exception? ex)
    {
        if (ex != null)
        {
            Console.WriteLine($"Message: {ex.Message}");
            Console.WriteLine("Stacktrace:");
            Console.WriteLine(ex.StackTrace);
            Console.WriteLine();
            PrintException(ex.InnerException);
        }
    }

    private static void SendMessageToQueue(string filePath)
    {
        var factory = new ConnectionFactory { HostName = "localhost" };
        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();
        channel.QueueDeclare(queue: "DataCaptureQueue",
                             durable: false,
                             exclusive: false,
                             autoDelete: false,
                             arguments: null);

        using (var fileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read))
        {
            const int chunkSize = 65536; //64KB
            byte[] buffer = new byte[chunkSize];

            int totalChunks = (int)Math.Ceiling((double)fileStream.Length / chunkSize);
            int chunkID = 1;

            var properties = channel.CreateBasicProperties();
            properties.Headers = new Dictionary<string, object>(){
                {"filePath", filePath },
                {"totalChunks", totalChunks}
            };
            System.Console.WriteLine($"File {filePath} with {fileStream.Length} bytes is ready for transfer.");
            int totalBytesRead = 0;
            int bytesRead;
            while ((bytesRead = fileStream.Read(buffer, 0, buffer.Length)) > 0)
            {
                totalBytesRead += bytesRead;
                properties.Headers["chunkID"] = chunkID++; // Update chunk ID for each message

                // Resize the buffer if the bytes read are less than the buffer size.
                if (bytesRead < buffer.Length)
                {
                    var temp = new byte[bytesRead];
                    Array.Copy(buffer, temp, bytesRead);
                    buffer = temp;
                }

                channel.BasicPublish(exchange: string.Empty,
                                     routingKey: "DataCaptureQueue",
                                     basicProperties: properties,
                                     body: buffer);
            }
            System.Console.WriteLine($"{totalBytesRead} bytes of file {filePath} has been sent.");
        }

        Console.WriteLine("File sent to message queue.");
    }
}
