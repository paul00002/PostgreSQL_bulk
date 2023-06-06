using SimpleJSON;

namespace Load_Parameters
{
    public class Parameters
    {
        public Parameters(StreamReader fileJson)
        {
            string textjson = fileJson.ReadToEnd();
            JSONNode j = JSONNode.Parse(textjson);
            adlsTenantId = j["TenantId"].ToString().Replace("\"", "");
            adlsClientId = j["ClientId"].ToString().Replace("\"", "");
            adlsClientSecret = j["ClientSecret"].ToString().Replace("\"", "");
            adlsURL_Connectin = j["URL_Connectin"].ToString().Replace("\"", "");
            adlsFileSystemName = j["FileSystemName"].ToString().Replace("\"", "");
            adlsPath = j["Path"].ToString().Replace("\"", "");

            //----------------------

            Host = j["Host"].ToString().Replace("\"", "");
            Username = j["Username"].ToString().Replace("\"", "");
            Database = j["Database"].ToString().Replace("\"", "");
            Port = j["Port"].AsInt;
            Password = j["Password"].ToString().Replace("\"", "");
            SSLMode = j["SSLMode"].ToString().Replace("\"", "");
            table_name = j["table_name"].ToString().Replace("\\\"", "&").Replace("\"", "").Replace("&", "\"");
            batch = j["batch"].AsInt;
            batch_size = j["batch_size"].AsInt;

        }

        public string adlsTenantId { get; set; }
        public string adlsClientId { get; set; }
        public string adlsClientSecret { get; set; }
        public string adlsURL_Connectin { get; set; }
        public string adlsFileSystemName { get; set; }
        public string adlsPath { get; set; }
        public string Host { get; set; }
        public string Username { get; set; }
        public string Database { get; set; }
        public int Port { get; set; }
        public string Password { get; set; }
        public string SSLMode { get; set; }
        public string table_name { get; set; }
        public int batch { get; set; }
        public int batch_size { get; set; }
    }
}
