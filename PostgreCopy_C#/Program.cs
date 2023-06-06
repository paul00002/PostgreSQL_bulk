using Load_Parameters;
using AzureDatalake;
using Azure.Storage.Files.DataLake;
using PostgresSQL;


class ValidateAzureConectionException : Exception
{
    public ValidateAzureConectionException()
        : base(String.Format("ValidateAzureConectionException check Connection Parameters"))
    {

    }
}


namespace PostgresCopy
{
    internal class Program
    {
        static void Main()
        {   
            string filepath=@"parameters.json";
            StreamReader fileJson = new StreamReader(filepath);
            Parameters parameters = new Parameters(fileJson);


            var dl= new Datalake(parameters.adlsTenantId, parameters.adlsClientId, parameters.adlsClientSecret, parameters.adlsURL_Connectin);
            DataLakeServiceClient conn = dl.Connect();
            var filesystemclient = conn.GetFileSystemClient(parameters.adlsFileSystemName);
            var filePath = filesystemclient.GetFileClient(parameters.adlsPath);
            StreamReader streamReade;
            try
            {
                streamReade = new StreamReader(filePath.Read().Value.Content);
            }
            catch (Exception ex)
            {
                
                throw new ValidateAzureConectionException();
            }


            Console.WriteLine("Import Started PostgresCopy: {0}", DateTime.UtcNow.ToString());
            
            postgreSQL pg = new postgreSQL(parameters.Host, parameters.Username, parameters.Database, parameters.Port, parameters.Password, parameters.SSLMode);
            if (parameters.batch == 0)
            {
                pg.CopyDataToPostgreSQL(streamReade, parameters.table_name);
            }
            else
            {
                pg.CopyDataToPostgreSQL_BY_batch(streamReade, parameters.table_name, parameters.batch_size);

            }
            
            Console.WriteLine("Import Ended PostgresCopy: {0}", DateTime.UtcNow.ToString());
        }
    }
}
