using Microsoft.Azure.Cosmos.Table;

using SujaySarma.Sdk.DataSources.AzureTables.Attributes;
using SujaySarma.Sdk.DataSources.AzureTables.PrivateReflector;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;

namespace SujaySarma.Sdk.DataSources.AzureTables
{
    /// <summary>
    /// Data source that helps interact with data stored in Azure/CosmosDB tables 
    /// using the Azure Tables API.
    /// </summary>
    public class AzureTablesDataSource
    {

        #region Properties

        /// <summary>
        /// Reference to the Azure Storage Account class
        /// </summary>
        public AzureStorageAccount StorageAccount { get; private set; } = null;

        /// <summary>
        /// Name of the current table
        /// </summary>
        public string CurrentTableName { get; private set; }

        #endregion

        /// <summary>
        /// Initialize the data source. Also ensures that the table exists.
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
            CurrentTableName = tableName;

            _currentTableClient = StorageAccount.GetCloudTableClient();
            _currentTableReference = _currentTableClient.GetTableReference(tableName);
            if (! _currentTableReference.Exists())
            {
                _currentTableReference.Create();
            }
        }

        #region Methods

        /// <summary>
        /// Executes a SELECT command against the table. Note that this method uses "yield return" for maximizing efficiency! 
        /// </summary>
        /// <typeparam name="T">Type of business object</typeparam>
        /// <param name="partitionKey">Value of the partition key (optional)</param>
        /// <param name="rowKey">Value of the row key (optional)</param>
        /// <param name="otherFilters">Other filters to use (optional). This should not contain the partition and row key filters if those are already provided 
        /// as the speciifc arguments (<paramref name="partitionKey"/> or <paramref name="rowKey"/>).</param>
        /// <param name="columnsList">List of columns to return. Set NULL to return everything.</param>
        /// <param name="orderByColumnName">Name of the column to sort results by (only if default sort - PK/RK - is not desirable). Only one column name is allowed.</param>
        /// <param name="isOrderByDescending">If true, performs a DESC sort by the <paramref name="orderByColumnName"/> column.</param>
        /// <param name="count">Number of results to return. Set zero or negative values to return all matches.</param>
        /// <returns>A lazily retrieved collection of business objects</returns>
        public IEnumerable<T> Select<T>(string partitionKey = null, string rowKey = null, string otherFilters = null, IEnumerable<string> columnsList = null, string orderByColumnName = null, bool isOrderByDescending = false, int count = -1)
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
                query.Append($" and ({otherFilters})");
            }

            TableQuery<AzureTableEntity> tableQuery = (new TableQuery<AzureTableEntity>()).Where(query.ToString());

            if (columnsList != null)
            {
                List<string> columnNamesToReturn = new List<string>();
                foreach (string item in columnsList)
                {
                    if ((!string.IsNullOrWhiteSpace(item)) && (!columnNamesToReturn.Contains(item)))
                    {
                        columnNamesToReturn.Add(item);
                    }
                }

                if (columnNamesToReturn.Count > 0)
                {
                    // Partition & Row key must always be selected, or we get weird results!

                    if (!columnNamesToReturn.Contains("PartitionKey"))
                    {
                        columnNamesToReturn.Add("PartitionKey");
                    }

                    if (!columnNamesToReturn.Contains("RowKey"))
                    {
                        columnNamesToReturn.Add("RowKey");
                    }

                    tableQuery.Select(columnNamesToReturn);
                }
            }

            if (!string.IsNullOrWhiteSpace(orderByColumnName))
            {
                tableQuery = (isOrderByDescending ? tableQuery.OrderByDesc(orderByColumnName) : tableQuery.OrderBy(orderByColumnName));
            }

            if (count > 0)
            {
                tableQuery = tableQuery.Take(count);
            }

            foreach (AzureTableEntity entity in _currentTableReference.ExecuteQuery(tableQuery))
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
        /// Insert the provided object into the table
        /// </summary>
        /// <typeparam name="T">Type of business object</typeparam>
        /// <param name="item">Business object</param>
        /// <returns>Zero if nothing was inserted, One if the object was inserted</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ulong Insert<T>(T item)
            where T : class
        {
            if (item != null)
            {
                ExecuteNonQuery<T>(TableOperation.Insert(AzureTableEntity.From(item)));
                return 1L;
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
        /// Update the provided object into the table
        /// </summary>
        /// <typeparam name="T">Type of business object</typeparam>
        /// <param name="item">Business object</param>
        /// <returns>Zero if nothing was updated, One if the object was updated</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ulong Update<T>(T item)
            where T : class
        {
            if (item != null)
            {
                ExecuteNonQuery<T>(TableOperation.Merge(AzureTableEntity.From(item)));
                return 1L;
            }

            return 0L;
        }

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
                ClassInformation objectInfo = TypeInspector.InspectForAzureTables<T>();
                if (objectInfo == null)
                {
                    throw new TypeLoadException($"Type '{typeof(T).FullName}' is not anotated with the '{typeof(TableAttribute).FullName}' attribute.");
                }

                if (objectInfo.TableAttribute.UseSoftDelete)
                {
                    return UpdateInternal(objects, true);
                }

                TableBatchOperation delete = new TableBatchOperation();
                foreach (T instance in objects)
                {
                    delete.Delete(AzureTableEntity.From(instance, true));
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
        /// Delete the provided object from the table
        /// </summary>
        /// <typeparam name="T">Type of business object</typeparam>
        /// <param name="item">Business object</param>
        /// <returns>Zero if nothing was deleted, One if the object was deleted</returns>
        public ulong Delete<T>(T item)
            where T : class
        {
            if (item != null)
            {
                // we need to check if we are soft-deleting!
                ClassInformation objectInfo = TypeInspector.InspectForAzureTables<T>();
                if (objectInfo == null)
                {
                    throw new TypeLoadException($"Type '{typeof(T).FullName}' is not anotated with the '{typeof(TableAttribute).FullName}' attribute.");
                }

                TableAttribute tableAttribute = objectInfo.TableAttribute;
                AzureTableEntity entity = AzureTableEntity.From(item);
                TableOperation updateOrDeleteOperation;

                if (tableAttribute.UseSoftDelete)
                {
                    entity.AddOrUpdateProperty(AzureTableEntity.PROPERTY_NAME_ISDELETED, true);
                    updateOrDeleteOperation = TableOperation.Merge(entity);
                }
                else
                {
                    updateOrDeleteOperation = TableOperation.Delete(entity);
                }

                ExecuteNonQuery<T>(updateOrDeleteOperation);
                return 1L;
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
            AzureTableEntity originalEntity = _currentTableReference.ExecuteQuery(tableQuery).FirstOrDefault();
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
            _currentTableReference.Execute(operation);
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
                TableBatchOperation batchPage = new TableBatchOperation();

                // all entities in a batch must have the same partition key:
                foreach (IEnumerable<TableOperation> operations in batchOperation.GroupBy(o => o.Entity.PartitionKey))
                {
                    // order elements in a partition by row key so that we reduce tablescans
                    foreach (TableOperation operation in operations.OrderBy(o => o.Entity.RowKey))
                    {
                        batchPage.Add(operation);
                        if (batchPage.Count == 100)
                        {
                            _currentTableReference.ExecuteBatch(batchPage);
                            batchPage.Clear();
                        }
                    }
                }

                // get the remaining
                if (batchPage.Count > 0)
                {
                    _currentTableReference.ExecuteBatch(batchPage);
                }
            }
        }

        /// <summary>
        /// Get a list of tables in the account
        /// </summary>
        /// <returns>IEnumerable of CloudTables</returns>
        public IEnumerable<CloudTable> ListTables() => _currentTableClient.ListTables();


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
                        ClassInformation objectInfo = TypeInspector.InspectForAzureTables<T>();
                        if (objectInfo == null)
                        {
                            throw new TypeLoadException($"Type '{typeof(T).FullName}' is not anotated with the '{typeof(TableAttribute).FullName}' attribute.");
                        }

                        return objectInfo.TableAttribute.TableName;
                    }
                );

        #endregion

        #region IDisposable Support

        public virtual void Dispose()
        {
            if (!alreadyDisposed)
            {
                _currentTableReference = null;
                _currentTableClient = null;

                alreadyDisposed = true;
            }
        }
        private bool alreadyDisposed = false; // To detect redundant calls

        #endregion

        private static readonly ConcurrentDictionary<string, string> entityTableNames = new ConcurrentDictionary<string, string>();
        
        private CloudTableClient _currentTableClient = null;
        private CloudTable _currentTableReference = null;
    }
}
