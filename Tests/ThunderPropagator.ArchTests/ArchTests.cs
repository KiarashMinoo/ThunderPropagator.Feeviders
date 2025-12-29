using System.Reflection;
using NetArchTest.Rules;

namespace ThunderPropagator.ArchTests;

public class ArchTests
{
    private readonly IList<Assembly> _assemblies = [];

    public ArchTests()
    {
        // ListAssemblies(typeof(SomeType).Assembly); // Placeholder, no specific assembly
    }

    private void ListAssemblies(Assembly assembly)
    {
        if (_assemblies.Contains(assembly))
            return;

        _assemblies.Add(assembly);

        foreach (var referencedAssembly in assembly.GetReferencedAssemblies())
            ListAssemblies(Assembly.Load(referencedAssembly));
    }

    // Placeholder test
    [Fact]
    public void PlaceholderTest()
    {
        Assert.True(true);
    }
}