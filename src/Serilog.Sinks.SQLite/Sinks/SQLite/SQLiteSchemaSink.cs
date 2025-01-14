﻿// Copyright 2016 Serilog Contributors
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SQLite;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Serilog.Core;
using Serilog.Debugging;
using Serilog.Events;
using Serilog.Sinks.Batch;
using Serilog.Sinks.Extensions;

namespace Serilog.Sinks.SQLite.Schema
{
    internal class SQLiteSchemaSink : BatchProvider, ILogEventSink, IDisposable
    {
        private readonly string _databasePath;
        private readonly bool _storeTimestampInUtc;
        private readonly uint _maxDatabaseSize;
        private readonly bool _rollOver;
        private readonly string _tableName;
        private readonly TimeSpan? _retentionPeriod;
        private readonly Timer _retentionTimer;
        private const string TimestampFormat = "yyyy-MM-ddTHH:mm:ss.fff";
        private const long BytesPerMb = 1_048_576;
        private const long MaxSupportedPages = 5_242_880;
        private const long MaxSupportedPageSize = 65536;
        private const long MaxSupportedDatabaseSize = unchecked(MaxSupportedPageSize * MaxSupportedPages) / 1048576;
        private static SemaphoreSlim semaphoreSlim = new SemaphoreSlim(1, 1);

        private readonly Dictionary<string, string> _schema;

        protected virtual void Dispose(bool disposing)
        {
            base.Dispose(disposing);
        }

        public SQLiteSchemaSink(
            string sqlLiteDbPath,
            string tableName,
            Dictionary<string, string> schema,
            bool storeTimestampInUtc,
            TimeSpan? retentionPeriod,
            TimeSpan? retentionCheckInterval,
            uint batchSize = 1000,
            uint maxDatabaseSize = 10000,
            bool rollOver = true)
            : base(batchSize: (int)batchSize, maxBufferSize: 100_000)
        {
            _databasePath = sqlLiteDbPath;
            _tableName = tableName;
            _storeTimestampInUtc = storeTimestampInUtc;
            _maxDatabaseSize = maxDatabaseSize;
            _rollOver = rollOver;

            // Default schema if none provided
            _schema = schema ?? new Dictionary<string, string>
            {
                {"Timestamp", "TEXT"},
                {"Level", "VARCHAR(16)"},
                {"Exception", "TEXT"},
                {"RenderedMessage", "TEXT"},
                {"Properties", "TEXT"}
            };

            if (maxDatabaseSize > MaxSupportedDatabaseSize)
            {
                throw new SQLiteException($"Database size greater than {MaxSupportedDatabaseSize} MB is not supported");
            }

            InitializeDatabase();

            if (!retentionPeriod.HasValue) 
                return;
            
            // impose a min retention period of 15 minute
            var retentionCheckMinutes = 15;
            if (retentionCheckInterval.HasValue)
            {
                retentionCheckMinutes = Math.Max(retentionCheckMinutes, retentionCheckInterval.Value.Minutes);
            }

            // impose multiple of 15 minute interval
            retentionCheckMinutes = (retentionCheckMinutes / 15) * 15;

            _retentionPeriod = new[] { retentionPeriod, TimeSpan.FromMinutes(30) }.Max();

            // check for retention at this interval - or use retentionPeriod if not specified
            _retentionTimer = new Timer(
                (x) => { ApplyRetentionPolicy(); },
                null,
                TimeSpan.FromMinutes(0),
                TimeSpan.FromMinutes(retentionCheckMinutes));
        }

        #region ILogEvent implementation

        public void Shutdown()
        {
            Dispose(false);
        }

        public void Emit(LogEvent logEvent)
        {
            PushEvent(logEvent);
        }

        #endregion

        private void InitializeDatabase()
        {
            using var conn = GetSqLiteConnection();
            CreateSqlTable(conn);
        }

        private SQLiteConnection GetSqLiteConnection()
        {
            var sqlConString = new SQLiteConnectionStringBuilder
            {
                DataSource = _databasePath,
                JournalMode = SQLiteJournalModeEnum.Memory,
                SyncMode = SynchronizationModes.Normal,
                CacheSize = 500,
                PageSize = (int)MaxSupportedPageSize,
                MaxPageCount = (int)(_maxDatabaseSize * BytesPerMb / MaxSupportedPageSize)
            }.ConnectionString;

            var sqLiteConnection = new SQLiteConnection(sqlConString);
            sqLiteConnection.Open();

            return sqLiteConnection;
        }

        private void CreateSqlTable(SQLiteConnection sqlConnection)
        {
            var colDefs = _schema.Select(kvp => $"{kvp.Key} {kvp.Value}").Aggregate((current, next) => $"{current}, {next}");
            var sqlCreateText = $"CREATE TABLE IF NOT EXISTS \"{_tableName}\"  ({colDefs})";
            var sqlCommand = new SQLiteCommand(sqlCreateText, sqlConnection);
            sqlCommand.ExecuteNonQuery();
        }

        private SQLiteCommand CreateSqlInsertCommand(SQLiteConnection connection)
        {
            var columns = string.Join(", ", _schema.Keys);
            var parameters = string.Join(", ", _schema.Keys.Select(key => $"@{key}"));
            var sqlInsertText = $"INSERT INTO {_tableName} ({columns}) VALUES ({parameters})";

            var sqlCommand = connection.CreateCommand();
            sqlCommand.CommandText = sqlInsertText;
            sqlCommand.CommandType = CommandType.Text;

            foreach (var key in _schema.Keys)
            {
                sqlCommand.Parameters.Add(new SQLiteParameter($"@{key}", DbType.String)); // You may want to map the correct DbType based on your schema
            }

            return sqlCommand;
        }


        private void ApplyRetentionPolicy()
        {
            var epoch = DateTimeOffset.Now.Subtract(_retentionPeriod.Value);
            using var sqlConnection = GetSqLiteConnection();
            using var cmd = CreateSqlDeleteCommand(sqlConnection, epoch);
            SelfLog.WriteLine("Deleting log entries older than {0}", epoch);
            var ret = cmd.ExecuteNonQuery();
            SelfLog.WriteLine($"{ret} records deleted");
        }

        private void TruncateLog(SQLiteConnection sqlConnection)
        {
            var cmd = sqlConnection.CreateCommand();
            cmd.CommandText = $"DELETE FROM {_tableName}";
            cmd.ExecuteNonQuery();

            VacuumDatabase(sqlConnection);
        }

        private static void VacuumDatabase(SQLiteConnection sqlConnection)
        {
            var cmd = sqlConnection.CreateCommand();
            cmd.CommandText = $"vacuum";
            cmd.ExecuteNonQuery();
        }

        private SQLiteCommand CreateSqlDeleteCommand(SQLiteConnection sqlConnection, DateTimeOffset epoch)
        {
            var cmd = sqlConnection.CreateCommand();
            cmd.CommandText = $"DELETE FROM {_tableName} WHERE Timestamp < @epoch";
            cmd.Parameters.Add(
                new SQLiteParameter("@epoch", DbType.DateTime2)
                {
                    Value = (_storeTimestampInUtc ? epoch.ToUniversalTime() : epoch).ToString(
                        TimestampFormat)
                });

            return cmd;
        }

        protected override async Task<bool> WriteLogEventAsync(ICollection<LogEvent> logEventsBatch)
        {
            if ((logEventsBatch == null) || (logEventsBatch.Count == 0))
                return true;
            await semaphoreSlim.WaitAsync().ConfigureAwait(false);
            try
            {
                await using var sqlConnection = GetSqLiteConnection();
                try
                {
                    await WriteToDatabaseAsync(logEventsBatch, sqlConnection).ConfigureAwait(false);
                    return true;
                }
                catch (SQLiteException e)
                {
                    SelfLog.WriteLine(e.Message);

                    if (e.ResultCode != SQLiteErrorCode.Full)
                        return false;

                    if (_rollOver == false)
                    {
                        SelfLog.WriteLine("Discarding log excessive of max database");

                        return true;
                    }

                    var dbExtension = Path.GetExtension(_databasePath);

                    var newFilePath = Path.Combine(Path.GetDirectoryName(_databasePath) ?? "Logs",
                        $"{Path.GetFileNameWithoutExtension(_databasePath)}-{DateTime.Now:yyyyMMdd_HHmmss.ff}{dbExtension}");
                         
                    File.Copy(_databasePath, newFilePath, true);

                    TruncateLog(sqlConnection);
                    await WriteToDatabaseAsync(logEventsBatch, sqlConnection).ConfigureAwait(false);

                    SelfLog.WriteLine($"Rolling database to {newFilePath}");
                    return true;
                }
                catch (Exception e)
                {
                    SelfLog.WriteLine(e.Message);
                    return false;
                }
            }
            finally
            {
                semaphoreSlim.Release();
            }
        }

        private async Task WriteToDatabaseAsync(IEnumerable<LogEvent> logEventsBatch, SQLiteConnection sqlConnection)
        {
            await using var tr = sqlConnection.BeginTransaction();
            await using var sqlCommand = CreateSqlInsertCommand(sqlConnection);
            sqlCommand.Transaction = tr;

            foreach (var logEvent in logEventsBatch)
            {
                // Assuming your schema key matches the LogEvent properties or you have a way to map them
                foreach (var kvp in _schema)
                {
                    var paramName = $"@{kvp.Key}";
                    if (kvp.Key.Equals("Timestamp", StringComparison.OrdinalIgnoreCase))
                    {
                        sqlCommand.Parameters[paramName].Value = _storeTimestampInUtc
                            ? logEvent.Timestamp.ToUniversalTime().ToString(TimestampFormat)
                            : logEvent.Timestamp.ToString(TimestampFormat);
                    }
                    else if (kvp.Key.Equals("Level", StringComparison.OrdinalIgnoreCase))
                    {
                        sqlCommand.Parameters[paramName].Value = logEvent.Level.ToString();
                    }
                    else if (kvp.Key.Equals("Exception", StringComparison.OrdinalIgnoreCase))
                    {
                        sqlCommand.Parameters[paramName].Value = logEvent.Exception?.ToString() ?? string.Empty;
                    }
                    else if (kvp.Key.Equals("RenderedMessage", StringComparison.OrdinalIgnoreCase))
                    {
                        sqlCommand.Parameters[paramName].Value = logEvent.MessageTemplate.Render(logEvent.Properties, null);
                    }
                    else if (kvp.Key.Equals("Properties", StringComparison.OrdinalIgnoreCase))
                    {
                        sqlCommand.Parameters[paramName].Value = logEvent.Properties.Count > 0
                            ? logEvent.Properties.Json()
                            : string.Empty;
                    }
                    else
                    {
                        sqlCommand.Parameters[paramName].Value = ExtractValue(logEvent, kvp.Key);
                    }
                }
                await sqlCommand.ExecuteNonQueryAsync().ConfigureAwait(false);
            }

            tr.Commit();
        }

        private static object ExtractValue(LogEvent logEvent, string key)
        {
            // Implement the logic to extract the value based on key from logEvent
            // For example, if your log event properties contain additional custom data
            return logEvent.Properties.TryGetValue(key, out var value) ? value.ToString() : null;
        }

    }
}
