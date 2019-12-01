﻿using Microsoft.Azure.Cosmos.Table;
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
            if ((type == TableOperationType.Invalid) || (type == TableOperationType.Retrieve))
            {
                throw new ArgumentOutOfRangeException("Unsupported operation for queue!");
            }
            
            if (businessObject == null)
            {
                throw new ArgumentNullException("businessObject");
            }

            var tableOperation = type switch
            {
                TableOperationType.Delete => TableOperation.Delete(AzureTableEntity.From(businessObject, forDelete: true)),
                TableOperationType.Insert => TableOperation.Insert(AzureTableEntity.From(businessObject)),
                TableOperationType.InsertOrMerge => TableOperation.InsertOrMerge(AzureTableEntity.From(businessObject)),
                TableOperationType.InsertOrReplace => TableOperation.InsertOrReplace(AzureTableEntity.From(businessObject)),
                TableOperationType.Merge => TableOperation.Merge(AzureTableEntity.From(businessObject)),
                TableOperationType.Replace => TableOperation.Replace(AzureTableEntity.From(businessObject)),
                _ => null,
            };

            TableOperation operation = tableOperation;
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
                if (!_queue.TryRemove(queueID, out TableOperationWrapper value))
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
        public OperationQueue(string connectionString)
        {
            tableClient = CloudStorageAccount.Parse(connectionString).CreateCloudTableClient();
            ResetQueueIndex();
            _timer = new Timer(OnFetchTimerElapsed, null, __TIMER_PERIOD, -1);
        }

        private readonly CloudTableClient tableClient;
        private readonly ConcurrentDictionary<ulong, TableOperationWrapper> _queue = new ConcurrentDictionary<ulong, TableOperationWrapper>();
        private readonly Queue<ulong> _queueOrder = new Queue<ulong>();
        private readonly Timer _timer;
        private readonly int __TIMER_PERIOD = 5000;
        private readonly Dictionary<string, CloudTable> tables = new Dictionary<string, CloudTable>();

        private ulong _queueIndex;

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
