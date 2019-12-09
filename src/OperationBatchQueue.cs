using Microsoft.Azure.Cosmos.Table;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;

namespace SujaySarma.Sdk.DataSources.AzureTables
{
    /// <summary>
    /// Allows BATCHED operations to be queued and forgotten. We only permit operations that do not 
    /// return a result. So, Insert/update/Delete and DDL queries are permitted. But, 
    /// SELECTs are not permitted.
    /// </summary>
    public class OperationBatchQueue
    {
        /// <summary>
        /// Queue a new operation
        /// </summary>
        /// <param name="batch">TableBatchOperation to queue</param>
        /// <param name="tableName">Name of the table to run it against</param>
        /// <returns>QueueID. Use it to remove the item later (if required)</returns>
        public void Add(TableBatchOperation batch, string tableName)
        {
            if ((batch == null) || (batch.Count == 0))
            {
                throw new ArgumentNullException("Empty batch");
            }

            if (string.IsNullOrWhiteSpace(tableName))
            {
                throw new ArgumentNullException("tableName");
            }

            // examine the batch, split into unique operations of a max of 100 elements each
            Dictionary<TableOperationType, TableBatchOperationWrapper> operationsByType = new Dictionary<TableOperationType, TableBatchOperationWrapper>()
            {
                { TableOperationType.Delete, new TableBatchOperationWrapper(new TableBatchOperation(), tableName) },
                { TableOperationType.Insert, new TableBatchOperationWrapper(new TableBatchOperation(), tableName) },
                { TableOperationType.InsertOrMerge, new TableBatchOperationWrapper(new TableBatchOperation(), tableName) },
                { TableOperationType.InsertOrReplace, new TableBatchOperationWrapper(new TableBatchOperation(), tableName) },
                { TableOperationType.Merge, new TableBatchOperationWrapper(new TableBatchOperation(), tableName) },
                { TableOperationType.Replace, new TableBatchOperationWrapper(new TableBatchOperation(), tableName) }
            };
            foreach (TableOperation operation in batch.OrderBy(o => o.Entity.RowKey).ThenBy(o => o.OperationType))
            {
                if ((operation.OperationType == TableOperationType.Invalid) || (operation.OperationType == TableOperationType.Retrieve))
                {
                    throw new ArgumentOutOfRangeException("Unsupported operation for queue!");
                }

                operationsByType[operation.OperationType].Batch.Add(operation);

                if (operationsByType[operation.OperationType].Batch.Count == 100)
                {
                    _queue.Enqueue(operationsByType[operation.OperationType]);
                    operationsByType[operation.OperationType] = new TableBatchOperationWrapper(new TableBatchOperation(), tableName);
                }
            }

            // next loop is for the next PK, flush the batches
            foreach (TableOperationType type in operationsByType.Keys)
            {
                if (operationsByType[type].Batch.Count > 0)
                {
                    _queue.Enqueue(operationsByType[type]);
                    operationsByType[type] = new TableBatchOperationWrapper(new TableBatchOperation(), tableName);
                }
            }
        }

        /// <summary>
        /// Queue a new operation
        /// </summary>
        /// <typeparam name="T">Type of businessObject</typeparam>
        /// <param name="listOfObjects">Business object instances - cannot be NULL.</param>
        /// <param name="type">Type of TableBatchOperation to generate</param>
        /// <returns>QueueID. Use it to remove the item later (if required)</returns>
        public void Add<T>(IEnumerable<T> listOfObjects, TableOperationType type) where T : class
        {
            int t = (int)type;
            if ((t < 0) || (t > 5))
            {
                throw new ArgumentOutOfRangeException("Unsupported operation for queue!");
            }

            if (listOfObjects == null)
            {
                throw new ArgumentNullException(nameof(listOfObjects));
            }

            string currentPartitionKey = Guid.NewGuid().ToString(); // nobody's partition key will ever be the same as this!
            TableBatchOperation batch = new TableBatchOperation();

            foreach(T obj in listOfObjects)
            {
                TableOperation tableOperation = type switch
                {
                    TableOperationType.Delete => TableOperation.Delete(AzureTableEntity.From(obj, forDelete: true)),
                    TableOperationType.Insert => TableOperation.Insert(AzureTableEntity.From(obj)),
                    TableOperationType.InsertOrMerge => TableOperation.InsertOrMerge(AzureTableEntity.From(obj)),
                    TableOperationType.InsertOrReplace => TableOperation.InsertOrReplace(AzureTableEntity.From(obj)),
                    TableOperationType.Merge => TableOperation.Merge(AzureTableEntity.From(obj)),
                    TableOperationType.Replace => TableOperation.Replace(AzureTableEntity.From(obj)),
                    _ => null,
                };

                // all items in a batch must be the same partition key
                // so if we hit a different one, we jump to a new batch
                switch (batch.Count)
                {
                    case 0:
                        currentPartitionKey = tableOperation.Entity.PartitionKey;
                        break;

                    default:
                        if (tableOperation.Entity.PartitionKey != currentPartitionKey)
                        {
                            _queue.Enqueue(new TableBatchOperationWrapper(batch, AzureTablesDataSource.GetTableName<T>()));
                            batch = new TableBatchOperation();

                            currentPartitionKey = tableOperation.Entity.PartitionKey;
                        }
                        break;
                }                

                batch.Add(tableOperation);

                if (batch.Count == 100)
                {
                    _queue.Enqueue(new TableBatchOperationWrapper(batch, AzureTablesDataSource.GetTableName<T>()));
                    batch = new TableBatchOperation();
                }
            }

            if (batch.Count > 0)
            {
                _queue.Enqueue(new TableBatchOperationWrapper(batch, AzureTablesDataSource.GetTableName<T>()));
            }
        }

        /// <summary>
        /// Event handler for the _timer's Elapsed event
        /// </summary>
        private void OnQueueProcessorTimerElapsed(object _)
        {
            if (_queue.Count == 0)
            {
                ResetTimer();
                return;
            }

            while (_queue.TryDequeue(out TableBatchOperationWrapper value))
            {
                try
                {
                    if (!tables.ContainsKey(value.TableName))
                    {
                        CloudTable tableReference = tableClient.GetTableReference(value.TableName);
                        if (!tableReference.Exists())
                        {
                            tableReference.Create();
                        }
                        tables.Add(value.TableName, tableReference);
                    }

                    tables[value.TableName].ExecuteBatch(value.Batch);
                }
                catch (Exception ex)
                {
                    System.Diagnostics.Trace.WriteLine($"[OperationBatchQueue]: Table name: {value.TableName}\r\nException: {ex.Message}.");

                    // eat the exception, we dont want to miss our next timer 
                    // because of improper operations in the queue!
                }
            }

            ResetTimer();
        }

        /// <summary>
        /// Resets the timer to wait another __TIMER_PERIOD milliseconds
        /// </summary>
        private void ResetTimer()
        {
            _timer.Change(__TIMER_PERIOD, -1);
        }

        /// <summary>
        /// Instantiate the queue
        /// </summary>
        /// <param name="connectionString">Connection string to the table storage</param>
        public OperationBatchQueue(string connectionString)
        {
            tableClient = CloudStorageAccount.Parse(connectionString).CreateCloudTableClient();
            _timer = new Timer(OnQueueProcessorTimerElapsed, null, __TIMER_PERIOD, -1);
        }

        private readonly CloudTableClient tableClient;
        private readonly ConcurrentQueue<TableBatchOperationWrapper> _queue = new ConcurrentQueue<TableBatchOperationWrapper>();
        private readonly Timer _timer;
        private readonly int __TIMER_PERIOD = 5000;
        private readonly Dictionary<string, CloudTable> tables = new Dictionary<string, CloudTable>();

        /// <summary>
        /// Wraps a TableBatchOperation so that we preserve the table it applies to
        /// </summary>
        private class TableBatchOperationWrapper
        {
            /// <summary>
            /// The operation
            /// </summary>
            public TableBatchOperation Batch
            {
                get;
                set;
            }

            /// <summary>
            /// Name of the table
            /// </summary>
            public string TableName
            {
                get;
                set;
            }

            /// <summary>
            /// Instantiate the object
            /// </summary>
            /// <param name="batch"></param>
            /// <param name="table"></param>
            public TableBatchOperationWrapper(TableBatchOperation batch, string table)
            {
                Batch = batch;
                TableName = table;
            }
        }

    }
}
