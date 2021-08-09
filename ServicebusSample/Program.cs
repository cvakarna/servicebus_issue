using Azure.Messaging.ServiceBus;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace ServicebusSample
{
    class Program
    {
        static void Main(string[] args)
        {
            Program p = new Program();
            Console.WriteLine("Hello World!");
            p.CreateConsumer().GetAwaiter().GetResult();
            Task.Delay(Timeout.Infinite).GetAwaiter().GetResult();

        }



        public async Task CreateConsumer()
        {
            //var client = new ServiceBusClient("fullyQualifiedNamespace", new DefaultAzureCredential());
            var client = new ServiceBusClient("connectionString");
            //lock duration configured to 5 min for the queue
            var proc = client.CreateProcessor("managedQueue", options: new ServiceBusProcessorOptions
            {
                MaxConcurrentCalls = 2,
                AutoCompleteMessages = false,
                MaxAutoLockRenewalDuration = new TimeSpan(0, 20, 0),//max auto renew 20 min
            });
            proc.ProcessMessageAsync += MessageHandler;
            proc.ProcessErrorAsync += ErrorHandler;
            Console.WriteLine("Max auto lock renew :" + proc.MaxAutoLockRenewalDuration);
            // start processing 
           await  proc.StartProcessingAsync();
        }


        async Task MessageHandler(ProcessMessageEventArgs args)
        {
            try
            {

                string body = args.Message.Body.ToString();
                Console.WriteLine($"Received: {body}");
                await Task.Delay(480000);//8 min long running job message should auto renew after
                                         //5min so that not access by other consumer since  we configured concurrent calls 2
               // complete the message after processing. messages is deleted from the queue. 
                await args.CompleteMessageAsync(args.Message);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception In Handler:" + ex);
            }

        }

        // handle any errors when receiving messages
        Task ErrorHandler(ProcessErrorEventArgs args)
        {
            Console.WriteLine(args.Exception.ToString());
            return Task.CompletedTask;
        }
    }
}
