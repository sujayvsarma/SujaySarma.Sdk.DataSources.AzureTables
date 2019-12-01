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
            Dictionary<TableOperationType, TableBatchOperationWrapper> operationCounts = new Dictionary<TableOperationType, TableBatchOperationWrapper>()
            {
                { TableOperationType.Delete, new TableBatchOperationWrapper(new TableBatchOperation(), tableName) },
                { TableOperationType.Insert, new TableBatchOperationWrapper(new TableBatchOperation(), tableName) },
                { TableOperationType.InsertOrMerge, new TableBatchOperationWrapper(new TableBatchOperation(), tableName) },
                { TableOperationType.InsertOrReplace, new TableBatchOperationWrapper(new TableBatchOperation(), tableName) },
                { TableOperationType.Merge, new TableBatchOperationWrapper(new TableBatchOperation(), tableName) },
                { TableOperationType.Replace, new TableBatchOperationWrapper(new TableBatchOperation(), tableName) }
            };

            foreach (IEnumerable<TableOperation> isoPKOperations in batch.GroupBy(o => o.Entity.PartitionKey))
            {
                foreach (TableOperation operation in isoPKOperations.OrderBy(o => o.Entity.RowKey).ThenBy(o => o.OperationType))
                {
                    if ((operation.OperationType == TableOperationType.Invalid) || (operation.OperationType == TableOperationType.Retrieve))
                    {
                        throw new ArgumentOutOfRangeException("Unsupported operation for queue!");
                    }

                    operationCounts[operation.OperationType].Batch.Add(operation);

                    if (operationCounts[operation.OperationType].Batch.Count == 100)
                    {
                        _queueOrder.Enqueue(_queueIndex);
                        _queue.TryAdd(_queueIndex++, operationCounts[operation.OperationType]);
                        operationCounts[operation.OperationType] = new TableBatchOperationWrapper(new TableBatchOperation(), tableName);
                    }
                }

                // next loop is for the next PK, flush the batches
                foreach(TableOperationType type in operationCounts.Keys)
                {
                    if (operationCounts[type].Batch.Count > 0)
                    {
                        _queueOrder.Enqueue(_queueIndex);
                        _queue.TryAdd(_queueIndex++, operationCounts[type]);
                        operationCounts[type] = new TableBatchOperationWrapper(new TableBatchOperation(), tableName);
                    }
                }
            }
        }

        /// <summary>
        /// Queue a new operation
        /// </summary>
        /// <typeparam name="T">Type of businessObject</typeparam>
        /// <param name="businessObjects">Business object instances - cannot be NULL.</param>
        /// <param name="type">Type of TableBatchOperation to generate</param>
        /// <returns>QueueID. Use it to remove the item later (if required)</returns>
        public ulong Add<T>(IEnumerable<T> businessObjects, TableOperationType type) where T : class
        {
            if ((type == TableOperationType.Invalid) || (type == TableOperationType.Retrieve))
            {
                throw new ArgumentOutOfRangeException("Unsupported operation for queue!");
            }

            if (businessObjects == null)
            {
                throw new ArgumentNullException("businessObject");
            }

            TableBatchOperation batch = new TableBatchOperation();
            foreach(T obj in businessObjects)
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

                batch.Add(tableOperation);
            }

            _queueOrder.Enqueue(_queueIndex);
            _queue.TryAdd(_queueIndex++, new TableBatchOperationWrapper(batch, AzureTablesDataSource.GetTableName<T>()));
            return _queueIndex;
        }

        /// <summary>
        /// Remove an operation from the queue (silent exit if operation is not in the queue/anymore).
        /// </summary>
        /// <param name="ID">QueueID returned by the Add() operation</param>
        public void Remove(ulong ID)
        {
            if (_queue.ContainsKey(ID))
            {
                _queue.TryRemove(ID, out TableBatchOperationWrapper _);
            }

            //NOTE: The QueueID still remains in the _queueOrder queue. We cannot remove it 
            //      from there, because that is a queue and we have to dequeue/requeue all other 
            //      items from in it... very messy. So just leave it in there.
            //      In any case, the OnFetchTimerElapsed's loop handles missing QueueIDs :-)
        }

        /// <summary>
        /// Clear the queue. Whatever remains is removed. 
        /// NOTE: Previously returned QueueIDs are invalid after this call !
        /// </summary>
        public void Clear()
        {
            ResetQueueIndex();
            _queue.Clear();
            _queueOrder.Clear();
        }

        /// <summary>
        /// Event handler for the _timer's Elapsed event
        /// </summary>
        private void OnFetchTimerElapsed(object _)
        {
            if (_queue.Count == 0)
            {
                ResetTimer();
                return;
            }

            while (_queueOrder.TryDequeue(out ulong queueID))
            {
                if (!_queue.TryRemove(queueID, out TableBatchOperationWrapper value))
                {
                    continue;
                }

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
                catch
                {
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
        /// Reset the _queueIndex
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ResetQueueIndex()
        {
            DateTime utcNow = DateTime.UtcNow;
            _queueIndex = ulong.Parse(utcNow.ToString("yyyyMMddHHmmss")) + Convert.ToUInt64(utcNow.Ticks);
        }

        /// <summary>
        /// Instantiate the queue
        /// </summary>
        /// <param name="connectionString">Connection string to the table storage</param>
        public OperationBatchQueue(string connectionString)
        {
            tableClient = CloudStorageAccount.Parse(connectionString).CreateCloudTableClient();
            ResetQueueIndex();
            _timer = new Timer(OnFetchTimerElapsed, null, __TIMER_PERIOD, -1);
        }

        private readonly CloudTableClient tableClient;
        private readonly ConcurrentDictionary<ulong, TableBatchOperationWrapper> _queue = new ConcurrentDictionary<ulong, TableBatchOperationWrapper>();
        private readonly Queue<ulong> _queueOrder = new Queue<ulong>();
        private readonly Timer _timer;
        private readonly int __TIMER_PERIOD = 5000;
        private readonly Dictionary<string, CloudTable> tables = new Dictionary<string, CloudTable>();

        private ulong _queueIndex;

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
