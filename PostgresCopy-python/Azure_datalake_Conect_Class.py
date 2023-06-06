from azure.storage.filedatalake import DataLakeServiceClient
from azure.identity import ClientSecretCredential


class datalake_Download: 
      def __init__(self) -> None:
           self.adlsTenantId = "adlsTenantId"
           self.adlsClientId = "adlsClientId"
           self.adlsClientSecret = "adlsClientSecret"
           self.adlsFileSystemName = "adlsFileSystemName "
           self.adlsPath = "adlsPath"
           self.adlsURL_Connectin="adlsURL_Connectin"

      def connection(self):
          self.token_credential = ClientSecretCredential(
                            self.adlsTenantId,
                            self.adlsClientId,
                            self.adlsClientSecret,
                             )
          self.datalake_service_client = DataLakeServiceClient(self.adlsURL_Connectin,credential=self.token_credential)
       
      def Download(self,path):
          file_system_client =self.datalake_service_client.get_file_system_client(file_system=self.adlsFileSystemName)
          file_client = file_system_client.get_file_client(self.adlsPath)
          download= file_client.download_file()
          with open(path,"wb") as f:
               download.readinto(f) 
               f.close()
      
if __name__=="__main__":
     dw=datalake_Download()
     dw.connection()
     dw.Download("new_file.csv")