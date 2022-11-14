using Azure.Storage.Queues;
using Azure.Storage.Queues.Models;

namespace Htf2022.Deelbezems.Infrastructure;

internal class QueueReader : BackgroundService
{
    private readonly ILogger<QueueReader> _logger;
    private readonly IConfiguration _configuration;
    private readonly IServiceProvider _serviceProvider;

    public QueueReader(ILogger<QueueReader> logger, IConfiguration configuration, IServiceProvider serviceProvider)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        _serviceProvider = serviceProvider;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var queueUri = _configuration["Queue:Uri"];
        if (string.IsNullOrWhiteSpace(queueUri))
        {
            _logger.LogWarning("No setting found in configuration with key \"Queue:Uri\"");
            return;
        }

        var client = new QueueClient(new Uri(_configuration["Queue:Uri"]));

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var response = await client.ReceiveMessageAsync(cancellationToken: stoppingToken);

                if (response is { Value: { } message })
                {
                    await ProcessMessage(client, message, stoppingToken);
                }
                else
                {
                    await Task.Delay(500, stoppingToken);
                }
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Failed to receive a message");
            }
        }
    }

    private async Task ProcessMessage(QueueClient client, QueueMessage message, CancellationToken cancellationToken)
    {
        var @event = message.MessageText;

        try
        {
            var handler = _serviceProvider.GetService<IBezemEventHandler>();
            if (handler is null)
            {
                _logger.LogWarning($"Could not inject a service implementing {nameof(IBezemEventHandler)}. Make sure the service is registered correctly.");
                return;
            }

            await (handler.Handle(@event, cancellationToken));

            try
            {
                await client.DeleteMessageAsync(message.MessageId, message.PopReceipt, cancellationToken);
            }
            catch (Exception e)
            {
                _logger.LogWarning(e, "Failed to delete message after a successful handle");
            }
        }
        catch (Exception handleException)
        {
            _logger.LogError(handleException, "Failed to handle message");

            try
            {
                await client.UpdateMessageAsync(message.MessageId, message.PopReceipt, message.MessageText, TimeSpan.Zero,
                    cancellationToken);
            }
            catch (Exception updateException)
            {
                _logger.LogWarning(updateException, "Failed to update message after failed handle");
            }
        }
    }
}