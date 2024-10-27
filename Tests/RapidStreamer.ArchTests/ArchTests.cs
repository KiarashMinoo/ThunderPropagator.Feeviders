using System.Reflection;
using NetArchTest.Rules;
using RapidStreamer.Web.Domain;
using RapidStreamer.Web.Models;

namespace RapidStreamer.ArchTests;

public class ArchTests
{
    private readonly IList<Assembly> _assemblies = [];

    public ArchTests()
    {
        ListAssemblies(typeof(ServerConfigurationModel).Assembly);
    }

    private void ListAssemblies(Assembly assembly)
    {
        if (_assemblies.Contains(assembly))
            return;

        _assemblies.Add(assembly);

        foreach (var referencedAssembly in assembly.GetReferencedAssemblies())
            ListAssemblies(Assembly.Load(referencedAssembly));
    }

    [Fact]
    public void DbModels_MustNotHave_PublicConstructor()
    {
        var invalidTypes = Types.InAssemblies(_assemblies).That().ImplementInterface(typeof(IDbModel)).And().AreNotAbstract().GetTypes().Where(type => type.GetConstructors().Length != 0);

        Assert.Empty(invalidTypes);
    }
}