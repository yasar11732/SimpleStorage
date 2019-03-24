using NUnit.Framework;
using System;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;

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
            Directory.Delete(d.data_directory);
        }

        [Test]
        public void DirectoryEntry_CanSerializeDeserialize([Random(5)] ulong key, [Random(5)] uint sector, [Random(5)] uint length)
        {
            DirectoryEntry d = new DirectoryEntry(key, sector, length);
            DirectoryEntry clone;
            byte[] buffer;

            buffer = d.Serialize();
            clone = new DirectoryEntry(buffer, 0);

            Assert.That(buffer.Length, Is.EqualTo(24));
            Assert.That(clone.Key, Is.EqualTo(d.Key));
            Assert.That(clone.FirstSector, Is.EqualTo(d.FirstSector));
            Assert.That(clone.Lengh, Is.EqualTo(d.Lengh));
            Assert.That(clone.CreationTime, Is.EqualTo(d.CreationTime));
        }
    }
}
