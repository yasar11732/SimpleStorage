using NUnit.Framework;
using System.IO;

namespace SimpleStorage.Tests
{
    [TestFixture]
    public class SimpleStorageTests
    {
        [Test]
        public void Database_ThrowsUnauthorizedAccessException()
        {
            string filename = "non-existent-path";
            Assert.That(() => new Database(filename),
                Throws.InstanceOf(typeof(System.UnauthorizedAccessException)));
        }
        [Test]
        public void Database_CanInstantiateWithGivenPath()
        {
            string filename = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
            Database d = new Database(filename);
            Assert.That(d.data_directory, Is.EqualTo(filename));
            Assert.That(d.data_directory, Does.Exist);
            Directory.Delete(d.data_directory, true);
        }

        [Test]
        public void Database_CanInstantiateWithNullPath()
        {
            Database d = new Database(null);
            Assert.That(d.data_directory, Is.Not.Null);
        }

    }
}
