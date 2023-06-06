using Npgsql;

class ValidatePostgreSQLConectionException : Exception
{
    public ValidatePostgreSQLConectionException()
        : base(String.Format("ValidateConectionException check Connection Parameters"))
    {

    }
}

namespace PostgresSQL
{   
    
    public class postgreSQL
    {
        public postgreSQL(string Host, string Username, string Dataase, int Port, string Password, string SSLMode)
        {
            PostgreSQL_Host = Host;
            PostgreSQL_Username = Username;
            PostgreSQL_Dataase = Dataase;
            PostgreSQL_Port = Port;
            PostgreSQL_Password = Password;
            PostgreSQL_SSLMode = SSLMode;
        }

        public void Connect()
        {
            string postgresConnectionString = String.Format("Server={0};Username={1};Database={2};Port={3};Password={4};SSLMode={5}",
                                                           PostgreSQL_Host,
                                                           PostgreSQL_Username,
                                                           PostgreSQL_Dataase,
                                                           PostgreSQL_Port,
                                                           PostgreSQL_Password,
                                                           PostgreSQL_SSLMode);
            PostgreSQL_postgreConnection = new NpgsqlConnection(postgresConnectionString);
           
        }

        public void CopyDataToPostgreSQL(StreamReader streamReader, string Table_name)
        {
            Connect();
            string Copy_query = String.Format("copy {0} from STDIN (FORMAT BINARY)", Table_name);
            using (streamReader)
            using (PostgreSQL_postgreConnection)
            {
                try
                {
                    PostgreSQL_postgreConnection.Open();
                }
                catch (Exception ex)
                {

                    throw new ValidatePostgreSQLConectionException();
                }
                
                string[] headers = streamReader.ReadLine()?.Split(';');

                using (var writer = PostgreSQL_postgreConnection.BeginBinaryImport(Copy_query))
                {
                    while (!streamReader.EndOfStream)
                    {
                        string[] rows = streamReader.ReadLine()?.Split(';');
                        if (rows != null)
                        {

                            writer.StartRow();
                            for (int i = 0; i < headers.Length; i++)
                            {
                                //Console.WriteLine(rows[i]);
                                writer.Write(rows[i]);
                            }
                        }
                    }
                    writer.Complete();
                }
                PostgreSQL_postgreConnection.Close();
            }

        }

        public void CopyDataToPostgreSQL_BY_batch(StreamReader streamReader, string Table_name, int Batch)
        {
            Connect();
            string Copy_query = String.Format("copy {0} from STDIN (FORMAT BINARY)", Table_name);
            try
            {
                PostgreSQL_postgreConnection.Open();
            }
            catch (Exception ex)
            {

                throw new ValidatePostgreSQLConectionException();
            }
            int b1 = Batch;
            string[] headers = streamReader.ReadLine()?.Split(';');

            var writer = PostgreSQL_postgreConnection.BeginBinaryImport(Copy_query);

            while (!streamReader.EndOfStream)
            {
                string[] rows = streamReader.ReadLine()?.Split(';');
                if (rows != null)
                {

                    writer.StartRow();
                    for (int i = 0; i < headers.Length; i++)
                    {
                        //Console.WriteLine(rows[i]);
                        writer.Write(rows[i]);
                    }
                }
                if (b1 == 0)
                {
                    writer.Complete();
                    PostgreSQL_postgreConnection.Close();
                    PostgreSQL_postgreConnection.Open();
                    writer = PostgreSQL_postgreConnection.BeginBinaryImport(Copy_query);
                    b1 = Batch;
                }
                b1--;
            }
            writer.Complete();

            PostgreSQL_postgreConnection.Close();

        }

        // Properties.
        private static string? PostgreSQL_Host { get; set; }
        private static string? PostgreSQL_Username { get; set; }
        private static string? PostgreSQL_Dataase { get; set; }
        private static int? PostgreSQL_Port { get; set; }
        private static string? PostgreSQL_Password { get; set; }
        private static string? PostgreSQL_SSLMode { get; set; }
        private static NpgsqlConnection? PostgreSQL_postgreConnection { get; set; }
        public override string ToString() => String.Format("Host={0}\nUsername={1}\nDatabase={2}\nPort={3}\nPassword={4}\nSSLMode={5}",
                                                           PostgreSQL_Host,
                                                           PostgreSQL_Username,
                                                           PostgreSQL_Dataase,
                                                           PostgreSQL_Port,
                                                           PostgreSQL_Password,
                                                           PostgreSQL_SSLMode);

    }
}
