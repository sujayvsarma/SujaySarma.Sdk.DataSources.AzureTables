using Microsoft.Azure.Cosmos.Table;

using SujaySarma.Sdk.Core.Reflection;
using SujaySarma.Sdk.DataSources.AzureTables.Attributes;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SujaySarma.Sdk.DataSources.AzureTables
{
    /// <summary>
    /// Data source that helps interact with data stored in Azure/CosmosDB tables using the classic Azure Tables API.
    /// </summary>
    public class AzureTablesDataSource
    {
        #region Properties

        /// <summary>
        /// Name of the database currently connected to
        /// </summary>
        public string DatabaseName { get; private set; } = null;

        /// <summary>
        /// The connection string for the current connection
        /// </summary>
        public string ConnectionString { get; private set; } = null;

        /// <summary>
        /// Flag indicating if the data source allows reading data
        /// </summary>
        public virtual bool CanRead => true;

        /// <summary>
        /// Flag indicating if the data source allows writing data
        /// </summary>
        public virtual bool CanWrite => true;

        /// <summary>
        /// Flag indicating if the data source allows deleting of data
        /// </summary>
        public virtual bool CanDelete => true;

        /// <summary>
        /// The Azure storage account
        /// </summary>
        public AzureStorageAccount StorageAccount { get; private set; } = null;

        /// <summary>
        /// The CloudTableClient used for interacting with the Azure/CosmosDB table system
        /// </summary>
        public CloudTableClient Connection { get; private set; } = null;

        /// <summary>
        /// The CloudTable currently connected to
        /// </summary>
        public CloudTable Table { get; private set; } = null;

        #endregion

        /// <summary>
        /// Initialize the data source
        /// </summary>
        /// <param name="storageConnectionString">Storage connection string</param>
        /// <param name="tableName">Name of the table to connect to</param>
        public AzureTablesDataSource(string storageConnectionString, string tableName)
        {
            if (string.IsNullOrWhiteSpace(storageConnectionString))
            {
                throw new ArgumentNullException(nameof(storageConnectionString));
            }

            if (string.IsNullOrWhiteSpace(tableName))
            {
                throw new ArgumentNullException(nameof(tableName));
            }

            StorageAccount = new AzureStorageAccount(storageConnectionString);
            ConnectionString = StorageAccount.ConnectionString;
            DatabaseName = tableName;

            Connection = StorageAccount.GetCloudTableClient();
        }

        #region Methods

        /// <summary>
        /// Change the connection to the provided database name (Table property is reset to the new table)
        /// </summary>
        /// <param name="newTableName">Name of another table to connect to</param>
        public void ChangeDatabase(string newTableName)
        {
            if (DatabaseName != null)
            {
                Close();
            }

            DatabaseName = newTableName;
            Open();
        }

        /// <summary>
        /// Close the connection (Table property is set to NULL)
        /// </summary>
        public void Close()
        {
            Table = null;
        }

        /// <summary>
        /// Opens a connection reference to the current <see cref="DatabaseName"/> and creates the table if it does not already exist.
        /// </summary>
        public void Open()
        {
            Table = Connection.GetTableReference(DatabaseName);
            Table.CreateIfNotExists();
        }

        /// <summary>
        /// Executes a SELECT command against the table. Note that this method uses "yield return" for maximizing efficiency! 
        /// </summary>
        /// <typeparam name="T">Type of business object</typeparam>
        /// <param name="partitionKey">Value of the partition key (optional)</param>
        /// <param name="rowKey">Value of the row key (optional)</param>
        /// <param name="otherFilters">Other filters to use (optional). This should not contain the partition and row key filters if those are already provided 
        /// as the speciifc arguments (<paramref name="partitionKey"/> or <paramref name="rowKey"/>).</param>
        /// <returns>A lazily retrieved collection of business objects</returns>
        public IEnumerable<T> Select<T>(string partitionKey = null, string rowKey = null, string otherFilters = null)
            where T : class, new()
        {
            StringBuilder query = new StringBuilder();
            query.Append("(IsDeleted eq false)");

            if (!string.IsNullOrWhiteSpace(partitionKey))
            {
                query.Append($" and (PartitionKey eq '{partitionKey}')");
            }

            if (!string.IsNullOrWhiteSpace(rowKey))
            {
                query.Append($" and (RowKey eq '{rowKey}')");
            }

            if (!string.IsNullOrWhiteSpace(otherFilters))
            {
                query.Append($"and ({otherFilters})");
            }

            TableQuery<AzureTableEntity> tableQuery = (new TableQuery<AzureTableEntity>()).Where(query.ToString());
            foreach (AzureTableEntity entity in GetTable<T>().ExecuteQuery(tableQuery))
            {
                yield return entity.To<T>();
            }
        }

        /// <summary>
        /// Inserts the provided objects into the table.
        /// </summary>
        /// <typeparam name="T">Type of business object</typeparam>
        /// <param name="objects">Collection of objects</param>
        public ulong Insert<T>(IEnumerable<T> objects)
            where T : class
        {
            if (objects != null)
            {
                TableBatchOperation insert = new TableBatchOperation();
                foreach (T instance in objects)
                {
                    insert.Insert(AzureTableEntity.From(instance));
                }

                ulong count = (ulong)insert.Count;
                if (count > 0)
                {
                    ExecuteNonQuery<T>(insert);
                    return count;
                }
            }

            return 0L;
        }

        /// <summary>
        /// Updates the provided objects into the table.
        /// </summary>
        /// <typeparam name="T">Type of business object</typeparam>
        /// <param name="objects">Collection of objects</param>
        public ulong Update<T>(IEnumerable<T> objects) where T : class => UpdateInternal(objects, false);


        /// <summary>
        /// Performs the actual UPDATE operation
        /// </summary>
        /// <typeparam name="T">Type of business object</typeparam>
        /// <param name="objects">Collection of objects</param>
        /// <param name="setSoftDeleteFlag">If TRUE, then we update the IsDeleted flag</param>
        private ulong UpdateInternal<T>(IEnumerable<T> objects, bool setSoftDeleteFlag = false)
            where T : class
        {
            if (objects != null)
            {
                TableBatchOperation update = new TableBatchOperation();
                foreach (T instance in objects)
                {
                    AzureTableEntity entity = AzureTableEntity.From(instance);
                    if (setSoftDeleteFlag)
                    {
                        entity.AddOrUpdateProperty(AzureTableEntity.PROPERTY_NAME_ISDELETED, true);
                    }
                    update.Merge(entity);
                }

                ulong count = (ulong)update.Count;
                if (count > 0)
                {
                    ExecuteNonQuery<T>(update);
                    return count;
                }
            }

            return 0L;
        }

        /// <summary>
        /// Delete the provided objects from the table.
        /// </summary>
        /// <typeparam name="T">Type of business object</typeparam>
        /// <param name="objects">Collection of objects</param>
        public ulong Delete<T>(IEnumerable<T> objects)
            where T : class
        {
            if (objects != null)
            {
                // we need to check if we are soft-deleting!
                ClassInfo objectInfo = TypeInspector.InspectOnlyIfAnotated<T, TableAttribute>();
                if (objectInfo == null)
                {
                    throw new TypeLoadException($"Type '{typeof(T).FullName}' is not anotated with the '{typeof(TableAttribute).FullName}' attribute.");
                }

                TableAttribute tableAttribute = objectInfo.GetAttributes<TableAttribute>().ToArray()[0];
                if (tableAttribute.UseSoftDelete)
                {
                    return UpdateInternal(objects, true);
                }

                TableBatchOperation delete = new TableBatchOperation();
                foreach (T instance in objects)
                {
                    delete.Delete(AzureTableEntity.From(instance));
                }

                ulong count = (ulong)delete.Count;
                if (count > 0)
                {
                    ExecuteNonQuery<T>(delete);
                    return count;
                }
            }

            return 0L;
        }

        /// <summary>
        /// Replace an entity with a new one. Especially useful when row/partition keys are being 
        /// modified or when the table row can contain columns not present in its entity representation (slice-tables)
        /// </summary>
        /// <typeparam name="T">Type of business object</typeparam>
        /// <param name="newCopy">The new data</param>
        /// <param name="originalPartitionKey">The partition key for the original object</param>
        /// <param name="originalRowKey">The row key for the original object</param>
        public void ReplaceWith<T>(T newCopy, string originalPartitionKey = null, string originalRowKey = null)
            where T : class
        {
            if ((newCopy == null) || (string.IsNullOrWhiteSpace(originalPartitionKey) && string.IsNullOrWhiteSpace(originalRowKey)))
            {
                throw new ArgumentNullException();
            }

            StringBuilder query = new StringBuilder();
            query.Append("(IsDeleted eq false)");

            if (!string.IsNullOrWhiteSpace(originalPartitionKey))
            {
                query.Append($" and (PartitionKey eq '{originalPartitionKey}')");
            }

            if (!string.IsNullOrWhiteSpace(originalRowKey))
            {
                query.Append($" and (RowKey eq '{originalRowKey}')");
            }

            TableQuery<AzureTableEntity> tableQuery = (new TableQuery<AzureTableEntity>()).Where(query.ToString());
            AzureTableEntity originalEntity = GetTable<T>().ExecuteQuery(tableQuery).FirstOrDefault();
            if (originalRowKey == default)
            {
                Insert(new T[] { newCopy });
                return;
            }

            TableOperation delete = TableOperation.Delete(originalEntity);
            ExecuteNonQuery<T>(delete);

            originalEntity.ImportValues(AzureTableEntity.From(newCopy));
            TableOperation insert = TableOperation.Insert(originalEntity);
            ExecuteNonQuery<T>(insert);
        }

        /// <summary>
        /// Execute a DML operation
        /// </summary>
        /// <typeparam name="T">Class type of the business object</typeparam>
        /// <param name="operation">TableOperation to execute</param>
        /// <param name="tableName">Name of the table. If not specified will be retrieved via ORM reflection</param>
        public void ExecuteNonQuery<T>(TableOperation operation, string tableName = null)
            where T : class
        {
            CloudTable table = null;
            if (string.IsNullOrWhiteSpace(tableName))
            {
                table = GetTable<T>();
            }
            else
            {
                if (!DatabaseName.Equals(tableName, StringComparison.InvariantCulture))
                {
                    ChangeDatabase(tableName);
                }
            }

            table.Execute(operation);
        }

        /// <summary>
        /// Execute a DML operation
        /// </summary>
        /// <typeparam name="T">Class type of the business object</typeparam>
        /// <param name="batchOperation">TableBatchOperation to execute</param>
        /// <param name="tableName">Name of the table. If not specified will be retrieved via ORM reflection</param>
        public void ExecuteNonQuery<T>(TableBatchOperation batchOperation, string tableName = null)
            where T : class
        {
            if (batchOperation.Count > 0)
            {
                CloudTable table = null;
                if (string.IsNullOrWhiteSpace(tableName))
                {
                    table = GetTable<T>();
                }
                else
                {
                    if (!DatabaseName.Equals(tableName, StringComparison.InvariantCulture))
                    {
                        ChangeDatabase(tableName);
                    }
                }

                table.ExecuteBatch(batchOperation);
            }
        }

        /// <summary>
        /// Returns a table reference for the provided class. The call is equivalent of calling 
        /// <see cref="ChangeDatabase(string)"/> for the table.
        /// </summary>
        /// <typeparam name="T">Type of object</typeparam>
        /// <returns>CloudTable reference, created if not exists</returns>
        public CloudTable GetTable<T>() where T : class
        {
            string tableName = GetTableName<T>();
            if ((Table == null) || (!DatabaseName.Equals(tableName, StringComparison.InvariantCulture)))
            {
                ChangeDatabase(tableName);
            }

            return Table;
        }

        /// <summary>
        /// Get the name of the table for the object
        /// </summary>
        /// <typeparam name="T">Class type of the business object</typeparam>
        /// <returns>Name of the table</returns>
        public static string GetTableName<T>() where T : class
            => entityTableNames.GetOrAdd(
                    typeof(T).FullName,
                    (objectName) =>
                    {
                        ClassInfo objectInfo = TypeInspector.InspectOnlyIfAnotated<T, TableAttribute>();
                        if (objectInfo == null)
                        {
                            throw new TypeLoadException($"Type '{typeof(T).FullName}' is not anotated with the '{typeof(TableAttribute).FullName}' attribute.");
                        }

                        return objectInfo.GetAttributes<TableAttribute>().ToArray()[0].TableName;
                    }
                );

        #endregion

        #region IDisposable Support

        public virtual void Dispose()
        {
            if (!alreadyDisposed)
            {
                Table = null;
                Connection = null;

                alreadyDisposed = true;
            }
        }
        private bool alreadyDisposed = false; // To detect redundant calls

        #endregion

        private static readonly ConcurrentDictionary<string, string> entityTableNames = new ConcurrentDictionary<string, string>();

    }
}
