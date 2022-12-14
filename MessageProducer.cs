using RabbitMQ.Client;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;

namespace DS2022_30442_Presecan_Alexandru_Assignment_2
{
    public class MessageProducer
    {
        int GetRandomDeviceId()
        {
            HttpClient httpClient = new HttpClient();
            httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", "eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJJZCI6ImEyMjFhNDhjLTQ4ZTktNGNiZC1hMTRkLTQxNWQwNzExZGFkYiIsInN1YiI6ImFkbWluIiwiZW1haWwiOiJhZG1pbkBnbWFpbC5jb20iLCJqdGkiOiIzMmEwZDA2OC1mMDhlLTRmMDQtYmQ3Yi04ODBiZGQ2M2UzMjkiLCJSb2xlIjoiQ2xpZW50IiwibmJmIjoxNjY3NzMzMDM1LCJleHAiOjE2Njc3MzM2MzUsImlhdCI6MTY2NzczMzAzNX0.qYGPP5JExZUu5H-vwfk_21j8VIJm87ajRVDDk8BmtwEYSP0Ayxv-xzh4Rm6giXk4fHC5YbzbnLu_aQoP7fkoAw");
            HttpResponseMessage httpResponseMessage = httpClient.GetAsync("http://localhost:7184/api/device/getRandomDeviceId").Result;

            if (!httpResponseMessage.IsSuccessStatusCode)
                return -1;

            return Convert.ToInt32(httpResponseMessage.Content.ReadAsStringAsync().Result);
        }

        List<float> GetValuesFromCsv()
        {
            return File
                .ReadAllLines("C:\\Users\\APresecan\\source\\repos\\DS2022_30442_Presecan_Alexandru_Assignment_2\\sensor.csv")
                .Select(value => Convert.ToSingle(value))
                .ToList();
        }

        void SendMessage(DateTime timeStamp, int deviceId, float energyConsumptionValue)
        {
            string message = JsonSerializer.Serialize(new
            {
                TimeStamp = timeStamp,
                DeviceId = deviceId,
                EnergyConsumptionValue = energyConsumptionValue
            });

            ConnectionFactory factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.QueueDeclare(queue: "sensor",
                                     durable: false,
                                     exclusive: false,
                                     autoDelete: false,
                                     arguments: null);

                channel.BasicPublish(exchange: "",
                                     routingKey: "sensor",
                                     basicProperties: null,
                                     body: Encoding.UTF8.GetBytes(message));
            }
        }

        public void Run(int milliseconds)
        {
            GetValuesFromCsv()
                .ForEach(value =>
                {
                    SendMessage(DateTime.Now, GetRandomDeviceId(), value);
                    Thread.Sleep(milliseconds);
                });
        }
    }
}
