using System.Threading.Channels;

namespace ThunderPropagator.Feeders.ActiveMQ
{
    internal sealed class ActiveMQMessageProcessor<TMessage>
    {
        private readonly Channel<TMessage> _messageChannel;
        private readonly Func<TMessage, Task> _processMessageAsync;
        private readonly Action<Exception> _handleError;
        private readonly Task _processingTask;

        public ActiveMQMessageProcessor(
            Func<TMessage, Task> processMessageAsync,
            Action<Exception> handleError)
        {
            _processMessageAsync = processMessageAsync;
            _handleError = handleError;
            _messageChannel = Channel.CreateUnbounded<TMessage>(new UnboundedChannelOptions
            {
                SingleReader = true,
                SingleWriter = false,
                AllowSynchronousContinuations = false
            });
            _processingTask = ProcessMessagesAsync();
        }

        public void Enqueue(TMessage message)
        {
            if (!_messageChannel.Writer.TryWrite(message))
                _handleError(new InvalidOperationException("Cannot process an ActiveMQ message after the processor has stopped."));
        }

        public void Complete() => _messageChannel.Writer.TryComplete();

        public async Task CompleteAsync()
        {
            Complete();
            await _processingTask.ConfigureAwait(false);
        }

        private async Task ProcessMessagesAsync()
        {
            await foreach (var message in _messageChannel.Reader.ReadAllAsync().ConfigureAwait(false))
            {
                try
                {
                    await _processMessageAsync(message).ConfigureAwait(false);
                }
                catch (Exception exception)
                {
                    _handleError(exception);
                }
            }
        }
    }
}
