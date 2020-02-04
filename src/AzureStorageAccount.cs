using System;


namespace SujaySarma.Sdk.DataSources.AzureTables
{

    /// <summary>
    /// Provides a common implementation for the Azure SDK's CloudStorageAccount. Multiple SDKs define independant versions of this 
    /// code with no inter-operability. This class provides the required translation without depending on any implementation. 
    /// </summary>
    public sealed class AzureStorageAccount
    {

        #region Properties

        /// <summary>
        /// If we are using the local development storage account
        /// </summary>
        public bool IsDevelopmentStorageAccount
        {
            get;
            private set;

        } = false;


        /// <summary>
        /// Account name 
        /// </summary>
        public string AccountName
        {
            get;
            private set;

        }

        /// <summary>
        /// Storage account key
        /// </summary>
        public string AccountKey
        {
            get;
            private set;

        }

        /// <summary>
        /// Uri to the Table service (CosmosDB has the same scheme)
        /// </summary>
        public Uri TableUri
            => (IsDevelopmentStorageAccount ? new Uri("http://127.0.0.1:10002/devstoreaccount1") : new Uri($"https://{AccountName}.{TableHostname}.{HostnameDomainName}/"));

        /// <summary>
        /// Recomposed connection string
        /// </summary>
        public string ConnectionString
            => $"DefaultEndpointsProtocol={(IsDevelopmentStorageAccount ? "http" : "https")};AccountName={AccountName};AccountKey={AccountKey};EndpointSuffix=core.windows.net";

        #endregion

        #region Constructors

        /// <summary>
        /// Initialize the account information
        /// </summary>
        /// <param name="connectionString">Connection string</param>
        public AzureStorageAccount(string connectionString)
        {
            if (string.IsNullOrWhiteSpace(connectionString))
            {
                throw new ArgumentException(connectionString);
            }

            IsDevelopmentStorageAccount = connectionString.StartsWith(UseDevelopmentStorage);
            AccountName = string.Empty;
            AccountKey = string.Empty;

            if (IsDevelopmentStorageAccount)
            {
                // we want to have things consistent below
                connectionString = DevelopmentStorageConnectionString;
                AccountName = DevelopmentStorageAccountName;
                AccountKey = DevelopmentStorageAccountKey;
            }

            foreach (string tokenSet in connectionString.Split(new char[] { ';' }, StringSplitOptions.RemoveEmptyEntries))
            {
                // we are only interested in two tokens
                if (tokenSet.StartsWith("AccountName"))
                {
                    AccountName = tokenSet.Split(new char[] { '=' }, StringSplitOptions.None)[1];
                    continue;
                }

                if (tokenSet.StartsWith("AccountKey"))
                {
                    // AccountKey is base64 encoded and will have "==" at the end
                    AccountKey = tokenSet[(tokenSet.IndexOf('=') + 1)..];
                    continue;
                }

                if ((!string.IsNullOrWhiteSpace(AccountName)) && (!string.IsNullOrWhiteSpace(AccountKey)))
                {
                    break;
                }
            }

            if (string.IsNullOrWhiteSpace(AccountName) || string.IsNullOrWhiteSpace(AccountKey))
            {
                throw new ArgumentException();
            }

            tableClient = new Microsoft.Azure.Cosmos.Table.CloudTableClient(
                    TableUri,
                    new Microsoft.Azure.Cosmos.Table.StorageCredentials(AccountName, AccountKey)
                );
        }

        #endregion

        #region Methods

        /// <summary>
        /// Create/returns the table client
        /// </summary>
        /// <returns>CloudTableClient</returns>
        public Microsoft.Azure.Cosmos.Table.CloudTableClient GetCloudTableClient()
        {
            return tableClient;
        }

        #endregion

        #region Private definitions

        private const string UseDevelopmentStorage = "UseDevelopmentStorage=true";
        private const string DevelopmentStorageConnectionString = "DefaultEndpointsProtocol=https;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;EndpointSuffix=core.windows.net";
        private const string TableHostname = "table";
        private const string HostnameDomainName = "core.windows.net";
        private const string DevelopmentStorageAccountName = "devstoreaccount1";
        private const string DevelopmentStorageAccountKey = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

        private readonly Microsoft.Azure.Cosmos.Table.CloudTableClient tableClient;

        #endregion

    }

}
