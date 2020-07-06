using Microsoft.Azure.Cosmos.Table;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
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
        public void Add(TableBatchOperation batch, string tableName)
        {
            if (_isDraining)
            {
                // no items can be added during a drain
                throw new Exception("Cannot queue items during a drain.");
            }

            if ((batch == null) || (batch.Count == 0))
            {
                throw new ArgumentNullException("Empty batch");
            }

            if (string.IsNullOrWhiteSpace(tableName))
            {
                throw new ArgumentNullException("tableName");
            }

            // examine the batch, split into unique operations of a max of 100 elements each
            foreach (IGrouping<string, TableOperation> operationByPartitionKeys in batch.GroupBy(b => b.Entity.PartitionKey))
            {
                string partitionKey = operationByPartitionKeys.Key;

                Dictionary<TableOperationType, TableBatchOperationWrapper> operationsByType = new Dictionary<TableOperationType, TableBatchOperationWrapper>()
                {
                    { TableOperationType.Delete, new TableBatchOperationWrapper(new TableBatchOperation(), tableName) },
                    { TableOperationType.Insert, new TableBatchOperationWrapper(new TableBatchOperation(), tableName) },
                    { TableOperationType.InsertOrMerge, new TableBatchOperationWrapper(new TableBatchOperation(), tableName) },
                    { TableOperationType.InsertOrReplace, new TableBatchOperationWrapper(new TableBatchOperation(), tableName) },
                    { TableOperationType.Merge, new TableBatchOperationWrapper(new TableBatchOperation(), tableName) },
                    { TableOperationType.Replace, new TableBatchOperationWrapper(new TableBatchOperation(), tableName) }
                };

                foreach (TableOperation operation in operationByPartitionKeys.OrderBy(o => o.Entity.RowKey).ThenBy(o => o.OperationType))
                {
                    if ((operation.OperationType == TableOperationType.Invalid) || (operation.OperationType == TableOperationType.Retrieve))
                    {
                        throw new ArgumentOutOfRangeException("Unsupported operation for queue!");
                    }

                    operationsByType[operation.OperationType].Batch.Add(operation);
                    ItemAdded?.Invoke(tableName, Enum.GetName(typeof(TableOperationType), operation.OperationType) ?? "Unknown", partitionKey, operation.Entity.RowKey);

                    if (operationsByType[operation.OperationType].Batch.Count == 100)
                    {
                        _queue.Enqueue(operationsByType[operation.OperationType]);
                        operationsByType[operation.OperationType] = new TableBatchOperationWrapper(new TableBatchOperation(), tableName);
                    }
                }

                // flush each op/group to the queue, because next iteration of the loop changes the partition key
                foreach (TableOperationType type in operationsByType.Keys)
                {
                    if (operationsByType[type].Batch.Count > 0)
                    {
                        _queue.Enqueue(operationsByType[type]);
                    }
                }
            }

        }

        /// <summary>
        /// Queue a new operation
        /// </summary>
        /// <typeparam name="T">Type of businessObject</typeparam>
        /// <param name="listOfObjects">Business object instances - cannot be NULL.</param>
        /// <param name="type">Type of TableBatchOperation to generate</param>
        public void Add<T>(IEnumerable<T> listOfObjects, TableOperationType type) where T : class
        {
            if (_isDraining)
            {
                // no items can be added during a drain
                throw new Exception("Cannot queue items during a drain.");
            }

            if (listOfObjects == null)
            {
                throw new ArgumentNullException(nameof(listOfObjects));
            }

            int t = (int)type;

            // these are the int values of the range of operation types supported
            if ((t < 0) || (t > 5))
            {
                throw new ArgumentOutOfRangeException("Unsupported operation for queue!");
            }

            string currentPartitionKey = Guid.NewGuid().ToString(); // nobody's partition key will ever be the same as this!
            string tableName = AzureTablesDataSource.GetTableName<T>();
            string operationName = Enum.GetName(typeof(TableOperationType), type) ?? "Unknown";

            TableBatchOperation batch = new TableBatchOperation();

            foreach (T obj in listOfObjects)
            {
                TableOperation tableOperation = type switch
                {
                    TableOperationType.Delete => TableOperation.Delete(AzureTableEntity.From(obj, forDelete: true)),
                    TableOperationType.Insert => TableOperation.Insert(AzureTableEntity.From(obj)),
                    TableOperationType.InsertOrMerge => TableOperation.InsertOrMerge(AzureTableEntity.From(obj)),
                    TableOperationType.InsertOrReplace => TableOperation.InsertOrReplace(AzureTableEntity.From(obj)),
                    TableOperationType.Merge => TableOperation.Merge(AzureTableEntity.From(obj)),
                    TableOperationType.Replace => TableOperation.Replace(AzureTableEntity.From(obj))

                    // Actually redundant, since this is already checked at the top of the function.
                    ,
                    _ => throw new ArgumentOutOfRangeException($"Unsupported operation '{operationName}'")
                };

                // all items in a batch must be the same partition key
                // so if we hit a different one, we jump to a new batch
                if ((batch.Count > 0) && (tableOperation.Entity.PartitionKey != currentPartitionKey))
                {
                    _queue.Enqueue(new TableBatchOperationWrapper(batch, tableName));

                    batch = new TableBatchOperation();
                    currentPartitionKey = tableOperation.Entity.PartitionKey;
                }
                else if (batch.Count == 0)
                {
                    currentPartitionKey = tableOperation.Entity.PartitionKey;
                }

                batch.Add(tableOperation);

                if (batch.Count == __MAX_ITEMS_PER_BATCH)
                {
                    _queue.Enqueue(new TableBatchOperationWrapper(batch, tableName));

                    batch = new TableBatchOperation();
                }

                ItemAdded?.Invoke(tableName, operationName, currentPartitionKey, tableOperation.Entity.RowKey);
            }

            // flush remaining entities to the queue
            if (batch.Count > 0)
            {
                _queue.Enqueue(new TableBatchOperationWrapper(batch, tableName));
            }
        }

        /// <summary>
        /// Number of elements currently in queue
        /// </summary>
        public int Count => _queue.Count;

        /// <summary>
        /// Returns if the queue currently contains items to be processed
        /// </summary>
        public bool HasItems => (_queue.Count > 0);

        /// <summary>
        /// Drain the queue immediately. Returns only after the queue is empty
        /// </summary>
        public void Drain()
        {
            _isDraining = true;

            while (_isTimerRunning)
            {
                Thread.Sleep(100);
            }

            _isTimerRunning = true;
            _timer.Change(Timeout.Infinite, Timeout.Infinite);

            DrainStarted?.Invoke(_queue.Count);

            if (_queue.Count > 0)
            {
                OnQueueProcessorTimerElapsed(null);
            }

            DrainComplete?.Invoke();
        }

        /// <summary>
        /// Unlike <see cref="Drain"/>, this method just clears the queue without persisting the changes.
        /// </summary>
        public void Clear()
        {
            if (_isDraining)
            {
                throw new InvalidOperationException("Cannot Clear when Drain() is running.");
            }

            // if timer is running, wait for it to finish
            while (_isTimerRunning)
            {
                Thread.Sleep(100);
            }

            _queue.Clear();
            Cleared?.Invoke();
        }

        /// <summary>
        /// Event handler for the _timer's Elapsed event
        /// </summary>
        private void OnQueueProcessorTimerElapsed(object? _)
        {
            if (_isTimerRunning && (!_isDraining))
            {
                return;
            }

            _isTimerRunning = true;

            if (_queue.Count > 0)
            {
                int counter = 0, total = _queue.Count;

                Stopwatch sw = new Stopwatch();
                sw.Start();

                while ((sw.ElapsedMilliseconds < __TIMER_RUN_TIME) && _queue.TryDequeue(out TableBatchOperationWrapper? value))
                {
                    string tableName = value.TableName;

                    try
                    {
                        if (!tables.ContainsKey(tableName))
                        {
                            CloudTable tableReference = tableClient.GetTableReference(tableName);
                            if (!tableReference.Exists())
                            {
                                tableReference.Create();
                            }
                            tables.Add(tableName, tableReference);
                        }

                        tables[tableName].ExecuteBatch(value.Batch);

                        TableOperation op = value.Batch[0];
                        Progress?.Invoke(++counter, total, tableName, Enum.GetName(typeof(TableOperationType), op.OperationType) ?? "Unknown", op.Entity.PartitionKey, op.Entity.RowKey);
                    }
                    catch (Exception ex)
                    {
                        Error?.Invoke(ex.Message, ex);

#if TRACE
                        Trace.WriteLine($"[OperationBatchQueue]: Table name: {tableName}\r\nException: {ex.Message}.");
#endif

                        // eat the exception, we dont want to miss our next timer 
                        // because of improper operations in the queue!
                    }
                }

                sw.Stop();
            }

            _isTimerRunning = false;
            _timer.Change(__TIMER_PERIOD, Timeout.Infinite);
        }

        /// <summary>
        /// Instantiate the queue
        /// </summary>
        /// <param name="connectionString">Connection string to the table storage</param>
        /// <param name="queueFlushInterval">Interval time in milliseconds between queue flushes (for eg: '1000' means the queue will be flushed once a second). 
        /// Set longer values for queues that do not see much CRUD operations.</param>
        public OperationBatchQueue(string connectionString, int queueFlushInterval = 1000)
        {
            tableClient = CloudStorageAccount.Parse(connectionString).CreateCloudTableClient();

            __TIMER_PERIOD = queueFlushInterval;
            _timer = new Timer(OnQueueProcessorTimerElapsed, null, __TIMER_PERIOD, Timeout.Infinite);
        }

        private readonly CloudTableClient tableClient;
        private readonly ConcurrentQueue<TableBatchOperationWrapper> _queue = new ConcurrentQueue<TableBatchOperationWrapper>();
        private readonly Timer _timer;
        private readonly int __TIMER_PERIOD = 1000, __MAX_ITEMS_PER_BATCH = 100, __TIMER_RUN_TIME = 30000;
        private readonly Dictionary<string, CloudTable> tables = new Dictionary<string, CloudTable>();

        private bool _isTimerRunning = false, _isDraining = false;

        /// <summary>
        /// Queue drain has completed
        /// </summary>
        public event CrudQueueDrainComplete? DrainComplete;

        /// <summary>
        /// Queue drain has begun
        /// </summary>
        public event CrudQueueDrainStarted? DrainStarted;

        /// <summary>
        /// An error has occurred
        /// </summary>
        public event CrudQueueException? Error;

        /// <summary>
        /// An item is being sent to Azure Storage
        /// </summary>
        public event CrudQueueFlushingItem? Progress;

        /// <summary>
        /// Queue was cleared of all items
        /// </summary>
        public event CrudQueueCleared? Cleared;

        /// <summary>
        /// An item was added to the event
        /// </summary>
        public event CrudQueueItemAdded? ItemAdded;

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

    /// <summary>
    /// Queue drain has begun
    /// </summary>
    /// <param name="numberOfElements">Number of elements in the queue at the start of the drain</param>
    public delegate void CrudQueueDrainStarted(int numberOfElements);

    /// <summary>
    /// Queue drain has completed
    /// </summary>
    public delegate void CrudQueueDrainComplete();

    /// <summary>
    /// The queue was cleared of all elements
    /// </summary>
    public delegate void CrudQueueCleared();

    /// <summary>
    /// A new item was enqueued
    /// </summary>
    /// <param name="tableName">Name of the table we are flushing this item to</param>
    /// <param name="operationName">Name of the CRUD operation (in lowercase)</param>
    /// <param name="partitionKey">The partition key of the item</param>
    /// <param name="rowKey">The row key of the item</param>
    public delegate void CrudQueueItemAdded(string tableName, string operationName, string partitionKey, string rowKey);

    /// <summary>
    /// Queue is flushing an item to storage
    /// </summary>
    /// <param name="item">Element number in this run</param>
    /// <param name="total">Total elements in the queue</param>
    /// <param name="tableName">Name of the table we are flushing this item to</param>
    /// <param name="operationName">Name of the CRUD operation (in lowercase)</param>
    /// <param name="partitionKey">The partition key of the item</param>
    /// <param name="rowKey">The row key of the item</param>
    public delegate void CrudQueueFlushingItem(int item, int total, string tableName, string operationName, string partitionKey, string rowKey);

    /// <summary>
    /// Exception event
    /// </summary>
    /// <param name="message">Message</param>
    /// <param name="exception">Exception instance (only if there was an downstream exception)</param>
    public delegate void CrudQueueException(string message, Exception? exception);
}
