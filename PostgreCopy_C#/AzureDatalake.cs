using Azure.Storage.Files.DataLake;
using Azure.Identity;


namespace AzureDatalake
{
    

    public class Datalake
    {
        public Datalake(string TenantId, string ClientId, string ClientSecret, string URL_Connectin)
        {
           
                Datalake_TenantId = TenantId;
                Datalake_ClientId = ClientId;
                Datalake_ClientSecret = ClientSecret;
                Datalake_URL_Connectin = URL_Connectin;

        }

        public DataLakeServiceClient Connect()
        {   

            ClientSecretCredential clientSecretCredential = new ClientSecretCredential(Datalake_TenantId, Datalake_ClientId, Datalake_ClientSecret);
            DataLakeServiceClient dataLakeServiceClient = new DataLakeServiceClient(new Uri(Datalake_URL_Connectin), clientSecretCredential);
            return dataLakeServiceClient;
        }


        // Properties.
        private static string? Datalake_TenantId { get; set; }
        private static string? Datalake_ClientId { get; set; } 
        private static string? Datalake_ClientSecret { get; set; } 
        private static string? Datalake_URL_Connectin { get; set; }

        public override string? ToString() => String.Format("TenantId= {0}\nClientId= {1}\nClientSecret={2}\nURL_Connectin={3}",
                                                           Datalake_TenantId,
                                                           Datalake_ClientId,
                                                           Datalake_ClientSecret,
                                                           Datalake_URL_Connectin);
    }



}