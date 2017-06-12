// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Server.Kestrel.Core.Internal.Infrastructure;
using Microsoft.AspNetCore.Server.Kestrel.Internal.System.IO.Pipelines;
using Microsoft.Extensions.Logging;

namespace Microsoft.AspNetCore.Server.Kestrel.Core.Internal.Http
{
    public class OutputProducer : IDisposable
    {
        private static readonly ArraySegment<byte> _emptyData = new ArraySegment<byte>(new byte[0]);

        private readonly string _connectionId;
        private readonly ITimeoutControl _timeoutControl;
        private readonly IKestrelTrace _log;

        // This locks access to to all of the below fields
        private readonly object _contextLock = new object();

        private bool _started = false;
        private bool _completed = false;

        private readonly IPipe _pipe;
        private readonly IPipe _innerPipe;

        // https://github.com/dotnet/corefxlab/issues/1334
        // Pipelines don't support multiple awaiters on flush
        // this is temporary until it does
        private TaskCompletionSource<object> _flushTcs;
        private readonly object _flushLock = new object();
        private Action _flushCompleted;

        public OutputProducer(IPipe pipe, string connectionId, ITimeoutControl timeoutControl, IKestrelTrace log, PipeFactory pipeFactory)
        {
            _pipe = pipe;
            _connectionId = connectionId;
            _timeoutControl = timeoutControl;
            _log = log;
            _flushCompleted = OnFlushCompleted;
            _innerPipe = pipeFactory.Create(new PipeOptions
            {
                ReaderScheduler = InlineScheduler.Default,
                WriterScheduler = InlineScheduler.Default,
                MaximumSizeHigh = 1,
                MaximumSizeLow = 1
            });
        }

        public Task WriteAsync(ArraySegment<byte> buffer, bool chunk = false, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (cancellationToken.IsCancellationRequested)
            {
                return Task.FromCanceled(cancellationToken);
            }

            return WriteAsync(buffer, cancellationToken, chunk);
        }

        public Task FlushAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            return WriteAsync(_emptyData, cancellationToken);
        }

        public void Write<T>(Action<WritableBuffer, T> callback, T state)
        {
            lock (_contextLock)
            {
                TryInit();

                if (_completed)
                {
                    return;
                }

                var buffer = _innerPipe.Writer.Alloc(1);
                callback(buffer, state);
                buffer.Commit();
            }
        }

        public void Dispose()
        {
            lock (_contextLock)
            {
                TryInit();

                if (_completed)
                {
                    return;
                }

                _log.ConnectionDisconnect(_connectionId);
                _completed = true;
                _innerPipe.Writer.Complete();
            }
        }

        public void Abort()
        {
            lock (_contextLock)
            {
                TryInit();

                if (_completed)
                {
                    return;
                }

                _log.ConnectionDisconnect(_connectionId);
                _completed = true;
                _innerPipe.Reader.CancelPendingRead();
                _innerPipe.Writer.Complete();
            }
        }

        private Task WriteAsync(
            ArraySegment<byte> buffer,
            CancellationToken cancellationToken,
            bool chunk = false)
        {
            var writableBuffer = default(WritableBuffer);

            lock (_contextLock)
            {
                TryInit();

                if (_completed)
                {
                    return Task.CompletedTask;
                }

                writableBuffer = _innerPipe.Writer.Alloc(1);
                var writer = new WritableBufferWriter(writableBuffer);
                if (buffer.Count > 0)
                {
                    if (chunk)
                    {
                        ChunkWriter.WriteBeginChunkBytes(ref writer, buffer.Count);
                    }

                    writer.Write(buffer.Array, buffer.Offset, buffer.Count);

                    if (chunk)
                    {
                        ChunkWriter.WriteEndChunkBytes(ref writer);
                    }
                }

                writableBuffer.Commit();
            }

            return FlushAsync(writableBuffer, cancellationToken);
        }

        private Task FlushAsync(WritableBuffer writableBuffer,
            CancellationToken cancellationToken)
        {
            var awaitable = writableBuffer.FlushAsync(cancellationToken);
            if (awaitable.IsCompleted)
            {
                // The flush task can't fail today
                return Task.CompletedTask;
            }
            return FlushAsyncAwaited(awaitable, cancellationToken);
        }

        private async Task FlushAsyncAwaited(WritableBufferAwaitable awaitable, CancellationToken cancellationToken)
        {
            // https://github.com/dotnet/corefxlab/issues/1334
            // Since the flush awaitable doesn't currently support multiple awaiters
            // we need to use a task to track the callbacks.
            // All awaiters get the same task
            lock (_flushLock)
            {
                if (_flushTcs == null || _flushTcs.Task.IsCompleted)
                {
                    _flushTcs = new TaskCompletionSource<object>();

                    awaitable.OnCompleted(_flushCompleted);
                }
            }
            await _flushTcs.Task;

            cancellationToken.ThrowIfCancellationRequested();
        }

        private void OnFlushCompleted()
        {
            _flushTcs.TrySetResult(null);
        }

        private void TryInit()
        {
            if (!_started)
            {
                _started = true;
                _ = PumpAsync();
            }
        }

        private async Task PumpAsync()
        {
            Exception error = null;

            try
            {
                var awaitable = _innerPipe.Reader.ReadAsync();

                while (true)
                {
                    var result = await awaitable;
                    var readableBuffer = result.Buffer;
                    var consumed = readableBuffer.Start;
                    var examined = readableBuffer.End;

                    try
                    {
                        if (_completed)
                        {
                            break;
                        }

                        if (!readableBuffer.IsEmpty)
                        {
                            var writableBuffer = _pipe.Writer.Alloc(1);

                            if (readableBuffer.IsSingleSpan)
                            {
                                writableBuffer.Write(readableBuffer.First.Span);
                            }
                            else
                            {
                                foreach (var memory in readableBuffer)
                                {
                                    writableBuffer.Write(memory.Span);
                                }
                            }

                            _timeoutControl.StartTimingWrite();
                            var w = writableBuffer.FlushAsync();

                            if (!w.IsCompleted)
                            {
                                _log.LogDebug("BACKPRESSURE");
                            }

                            await w;

                            if (_timeoutControl.TimedOut)
                            {
                                throw new TimeoutException();
                            }

                            _timeoutControl.StopTimingWrite();
                        }
                        else if (result.IsCompleted)
                        {
                            break;
                        }

                        awaitable = _innerPipe.Reader.ReadAsync();
                    }
                    finally
                    {
                        _innerPipe.Reader.Advance(readableBuffer.End);
                    }
                }
            }
            catch (Exception ex)
            {
                error = ex;
            }
            finally
            {
                _pipe.Writer.Complete(error);
            }
        }
    }
}
