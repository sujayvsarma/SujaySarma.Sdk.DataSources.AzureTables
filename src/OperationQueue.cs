using Microsoft.Azure.Cosmos.Table;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;

namespace SujaySarma.Sdk.DataSources.AzureTables
{
    /// <summary>
    /// Allows operations to be queued and forgotten. We only permit operations that do not 
    /// return a result. So, Insert/update/Delete and DDL queries are permitted. But, 
    /// SELECTs are not permitted.
    /// </summary>
    public class OperationQueue
    {
        /// <summary>
        /// Queue a new operation
        /// </summary>
        /// <param name="operation">TableOperation to queue</param>
        /// <param name="tableName">Name of the table to run it against</param>
        /// <returns>QueueID. Use it to remove the item later (if required)</returns>
        public ulong Add(TableOperation operation, string tableName)
        {
            if (_isDraining)
            {
                // no items can be added during a drain
                throw new Exception("Cannot queue items during a drain.");
            }

            if ((operation == null) || (operation.OperationType == TableOperationType.Invalid) || (operation.OperationType == TableOperationType.Retrieve))
            {
                throw new ArgumentOutOfRangeException("Unsupported operation for queue!");
            }
            if (string.IsNullOrWhiteSpace(tableName))
            {
                throw new ArgumentNullException("tableName");
            }

            _queue.TryAdd(_queueIndex++, new TableOperationWrapper(operation, tableName));
            return _queueIndex;
        }

        /// <summary>
        /// Queue a new operation
        /// </summary>
        /// <typeparam name="T">Type of businessObject</typeparam>
        /// <param name="businessObject">Business object instance - cannot be NULL.</param>
        /// <param name="type">Type of TableOperation to generate</param>
        /// <returns>QueueID. Use it to remove the item later (if required)</returns>
        public ulong Add<T>(T businessObject, TableOperationType type) where T : class
        {
            if (_isDraining)
            {
                // no items can be added during a drain
                throw new Exception("Cannot queue items during a drain.");
            }

            if ((type == TableOperationType.Invalid) || (type == TableOperationType.Retrieve))
            {
                throw new ArgumentOutOfRangeException("Unsupported operation for queue!");
            }

            if (businessObject == null)
            {
                throw new ArgumentNullException("businessObject");
            }

#pragma warning disable CS8509 // The switch expression does not handle all possible values of its input type (it is not exhaustive).
            TableOperation operation = type switch
            {
                TableOperationType.Delete => TableOperation.Delete(AzureTableEntity.From(businessObject, forDelete: true)),
                TableOperationType.Insert => TableOperation.Insert(AzureTableEntity.From(businessObject)),
                TableOperationType.InsertOrMerge => TableOperation.InsertOrMerge(AzureTableEntity.From(businessObject)),
                TableOperationType.InsertOrReplace => TableOperation.InsertOrReplace(AzureTableEntity.From(businessObject)),
                TableOperationType.Merge => TableOperation.Merge(AzureTableEntity.From(businessObject)),
                TableOperationType.Replace => TableOperation.Replace(AzureTableEntity.From(businessObject))
            };
#pragma warning restore CS8509 // The switch expression does not handle all possible values of its input type (it is not exhaustive).

            _queueOrder.Enqueue(_queueIndex);

            _queue.TryAdd(_queueIndex++, new TableOperationWrapper(operation, AzureTablesDataSource.GetTableName<T>()));
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
                _queue.TryRemove(ID, out TableOperationWrapper _);
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
            if (_isDraining)
            {
                // no items can be added during a drain
                throw new Exception("Cannot clear queue during a drain.");
            }

            ResetQueueIndex();
            _queue.Clear();
            _queueOrder.Clear();
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

            if (_queue.Count == 0)
            {
                return;
            }

            _isTimerRunning = true;
            _processQueueTimer.Change(Timeout.Infinite, Timeout.Infinite);

            OnProcessQueueTimerElapsed(null);
        }

        /// <summary>
        /// Event handler for the _timer's Elapsed event
        /// </summary>
        private void OnProcessQueueTimerElapsed(object? _)
        {
            if (_isTimerRunning && (!_isDraining))
            {
                return;
            }

            _isTimerRunning = true;

            if (_queue.Count == 0)
            {
                ResetTimer();
                return;
            }

            while (_queueOrder.TryDequeue(out ulong queueID))
            {
                if (!_queue.TryRemove(queueID, out TableOperationWrapper? value))
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

                    tables[value.TableName].Execute(value.Operation);
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
            if (!_isDraining)
            {
                _processQueueTimer.Change(__TIMER_PERIOD, -1);
                _isTimerRunning = false;
            }
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
        public OperationQueue(string connectionString)
        {
            tableClient = CloudStorageAccount.Parse(connectionString).CreateCloudTableClient();
            ResetQueueIndex();
            _processQueueTimer = new Timer(OnProcessQueueTimerElapsed, null, __TIMER_PERIOD, -1);
        }

        private readonly CloudTableClient tableClient;
        private readonly ConcurrentDictionary<ulong, TableOperationWrapper> _queue = new ConcurrentDictionary<ulong, TableOperationWrapper>();
        private readonly Queue<ulong> _queueOrder = new Queue<ulong>();
        private readonly Timer _processQueueTimer;
        private readonly int __TIMER_PERIOD = 5000;
        private readonly Dictionary<string, CloudTable> tables = new Dictionary<string, CloudTable>();

        private ulong _queueIndex;
        private bool _isTimerRunning = false, _isDraining = false;

        /// <summary>
        /// Wraps a TableOperation so that we preserve the table it applies to
        /// </summary>
        private class TableOperationWrapper
        {
            /// <summary>
            /// The operation
            /// </summary>
            public TableOperation Operation
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
            /// <param name="operation"></param>
            /// <param name="table"></param>
            public TableOperationWrapper(TableOperation operation, string table)
            {
                Operation = operation;
                TableName = table;
            }
        }

    }
}
