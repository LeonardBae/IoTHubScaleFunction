using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Management.IotHub;
using Microsoft.Azure.Management.IotHub.Models;
using Microsoft.Rest;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Microsoft.Rest.Azure;
using SendGrid;
using SendGrid.Helpers.Mail;

namespace IotHubScale
{
    public static class IotHubScale
    {
        // function configuration and authentication data
        // hard coded for the sample.  For production, look at something like KeyVault for storing secrets
        // more info here-> https://blogs.msdn.microsoft.com/dotnet/2016/10/03/storing-and-using-secrets-in-azure/
        static string ApplicationId = "16402570-c16a-49f6-81de-6278b571b25b";
        static string SubscriptionId = "668a2005-a0e8-4fdf-bba7-e1d6b3070c46";
        static string TenantId = "9eac23c9-656c-4bce-823e-3f91bedc50ee";
        static string ApplicationPassword = "p3rj-n11I0XZ-VamKLlYq~lc7Gs-NyAga~";
        static string ResourceGroupName = "JHIoTTEST-RG";
        static string IotHubName = "JHIoTTEST-hub";
        static int ThresholdPercentage = 90;
        static string SendGridAPIKey = "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";

        // "launcher" function.  runs periodically on timer trigger and just makes sure one (and only one)
        // instance of the orchestrator is running
        [FunctionName("IotHubScaleDownInit")]
        public static async Task IotHubScaleInit(
                [TimerTrigger("0 30 23 * * *")]TimerInfo myTimer,
                TraceWriter log)
        {
            log.Info($"C# Timer trigger function executed at: {DateTime.Now}");

            // connect management lib to iotHub
            IotHubClient client = GetNewIotHubClient(log);
            if (client == null)
            {
                log.Error("Unable to create IotHub client");
                return;
            }

            // get IotHub properties, the most important of which for our use is the current Sku details
            IotHubDescription desc = client.IotHubResource.Get(ResourceGroupName, IotHubName);
            string currentSKU = desc.Sku.Name;
            long currentUnits = desc.Sku.Capacity;

            // get current "used" message count for the IotHub
            long currentMessageCount = -1;
            IPage<IotHubQuotaMetricInfo> mi = client.IotHubResource.GetQuotaMetrics(ResourceGroupName, IotHubName);
            foreach (IotHubQuotaMetricInfo info in mi)
            {
                if (info.Name == "TotalMessages")
                    currentMessageCount = (long)info.CurrentValue;
            }
            if (currentMessageCount < 0)
            {
                log.Error("Unable to retreive current message count for IoTHub");
                return;
            }

            // compute the desired message threshold for the current sku
            long messageLimit = GetSkuUnitThreshold(desc.Sku.Name, desc.Sku.Capacity, ThresholdPercentage);

            log.Info("Current SKU Tier: " + desc.Sku.Tier);
            log.Info("Current SKU Name: " + currentSKU);
            log.Info("Current SKU Capacity: " + currentUnits.ToString());
            log.Info("Current Message Count:  " + currentMessageCount.ToString());
            log.Info("Current Sku/Unit Message Threshold:  " + messageLimit);

            // if we are below the threshold, nothing to do, bail
            if (currentMessageCount > messageLimit)
            {
                log.Info(String.Format("Current message count of {0} is more than the threshold of {1}. Nothing to do", currentMessageCount.ToString(), messageLimit));
                return;
            }
            else
                log.Info(String.Format("Current message count of {0} is over the threshold of {1}. Need to scale down IotHub", currentMessageCount.ToString(), messageLimit));

            // figure out what new sku level and 'units' we need to scale to
            string newSkuName = desc.Sku.Name;
            long newSkuUnits = GetScaleDownTarget(desc.Sku.Name, desc.Sku.Capacity);
            if (newSkuUnits < 0)
            {
                log.Error("Unable to determine new scale units for IoTHub (perhaps you are already at the highest units for a tier?)");
                return;
            }
            if (newSkuUnits == 0 && newSkuName == "S1")
                return;
            if (newSkuUnits == 0 && newSkuName == "S2")
            {
                newSkuName = "S1";
                newSkuUnits = 9;
            }
            else if (newSkuUnits == 0 && newSkuName == "S3")
            {
                newSkuName = "S2";
                newSkuUnits = 9;
            }
            // update the IoT Hub description with the new sku level and units
            desc.Sku.Name = newSkuName;
            desc.Sku.Capacity = newSkuUnits;

            // scale the IoT Hub by submitting the new configuration (tier and units)
            DateTime dtStart = DateTime.Now;
            client.IotHubResource.CreateOrUpdate(ResourceGroupName, IotHubName, desc);
            TimeSpan ts = new TimeSpan(DateTime.Now.Ticks - dtStart.Ticks);

            await SendMailAsync(desc.Sku.Name, desc.Sku.Capacity);
            log.Info(String.Format("Updated IoTHub {0} from {1}-{2} to {3}-{4} in {5} seconds", IotHubName, currentSKU, currentUnits, newSkuName, newSkuUnits, ts.Seconds));

            //  this would be a good place to send notifications that you scaled up the hub :-)
        }
        static async Task SendMailAsync(string Name, long Capacity)
        {
            var client = new SendGridClient(SendGridAPIKey);
            var msg = new SendGridMessage()
            {
                From = new EmailAddress("test@example.com", "DX Team"),
                Subject = "IoT Hub Scale down to " + Name + " and " + Capacity + " Units.",
                PlainTextContent = "Hello, Email!",
                HtmlContent = "<strong>Hello, Email!</strong>"
            };
            msg.AddTo(new EmailAddress("quiett20@Gmail.com", "Test User"));
            var response = await client.SendEmailAsync(msg);
        }
        // authenticate to Azure AD and get a token to acccess the the IoT Hub on behalf of our "application"
        private static IotHubClient GetNewIotHubClient(TraceWriter log)
        {
            var authContext = new AuthenticationContext(string.Format("https://login.microsoftonline.com/{0}", TenantId));
            var credential = new ClientCredential(ApplicationId, ApplicationPassword);
            AuthenticationResult token = authContext.AcquireTokenAsync("https://management.core.windows.net/", credential).Result;
            if (token == null)
            {
                log.Error("Failed to obtain the authentication token");
                return null;
            }

            var creds = new TokenCredentials(token.AccessToken);
            var client = new IotHubClient(creds);
            client.SubscriptionId = SubscriptionId;

            return client;
        }

        // get the new sku/units target for scaling the IoT Hub
        public static long GetScaleDownTarget(string currentSku, long currentUnits)
        {
            switch (currentSku)
            {
                case "S1":
                    if (currentUnits >= 1)  // 200 units is the maximum for S1 without involving Azure support
                        return --currentUnits;
                    else
                        return -1;
                case "S2":
                    if (currentUnits >= 1)  // 200 units is the maximum for S2 without involving Azure support
                        return --currentUnits;
                    else
                        return -1;
                case "S3":
                    if (currentUnits >= 1)  // can't have more than 10 S3 units without involving Azure support
                        return --currentUnits;
                    else
                        return -1;
            }
            return -1;   // shouldn't get here unless an invalid Sku was specified
        }

        // get the number of messages/day for the sku/unit/threshold combination
        public static long GetSkuUnitThreshold(string sku, long units, int percent)
        {
            long multiplier = 0;
            switch (sku)
            {
                case "S1":
                    multiplier = 400000;
                    break;
                case "S2":
                    if (units == 1)
                    {
                        multiplier = 400000;
                        units = 10;
                    }
                    else
                        multiplier = 6000000; 
                    break;
                case "S3":
                    if (units == 1)
                    {
                        multiplier = 6000000;
                        units = 10;
                    }
                    else
                        multiplier = 300000000;
                    break;
            }
            return (long)(multiplier * (units - 1) * percent) / 100;
        }
    }
}
