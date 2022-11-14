namespace Htf2022.Deelbezems.Infrastructure;

internal interface IBezemEventHandler
{
    Task Handle(string data, CancellationToken cancellationToken);
}