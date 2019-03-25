using NUnit.Framework;
using System;
using System.IO;
using System.IO.Abstractions.TestingHelpers;
using System.Runtime.Serialization.Formatters.Binary;

namespace SimpleStorage.Tests
{
    [TestFixture]
    public class DatabaseTests
    {
        Database d;
        [SetUp]
        public void SetUp()
        {
            d = new Database(null);
        }

        [TearDown]
        public void TearDown()
        {
            var _data_dir = d.data_directory;
            d.Dispose();
            d = null;
            Directory.Delete(_data_dir, true);
        }

        [Test]
        public void Database_CannotStartSecondCopy()
        {
            Assert.That(() => new Database(d.data_directory), Throws.Exception);
        }

        [Test]
        public void Database_CanInstantiate()
        {
            Assert.That(d.data_directory, Is.Not.Null);
        }
    }
    [TestFixture]
    public class SimpleStorageTests
    {

        [Test]
        public void Database_CanInstantiateWithGivenPath()
        {

            string filename = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
            Database d = new Database(filename);
            Assert.That(d.data_directory, Is.EqualTo(filename));
            Assert.That(Directory.Exists(d.data_directory), Is.True);
        }

        [Test]
        public void DirectoryEntry_CanSerializeDeserialize([Random(2)] ulong key, [Random(2)] uint sector, [Random(2)] uint length)
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

        [Test]
        public void Collection_CreatesDefaultFiles()
        {
            Database d = new Database(null);
            d.Put("test_collection", 0, null);
            var _data_dir = d.data_directory;
            var index_location = Path.Combine(_data_dir, "test_collection.index");
            var data_location = Path.Combine(_data_dir, "test_collection.data");
            var alloc_location = Path.Combine(_data_dir, "test_collection.alloc");

            Assert.That(index_location, Does.Exist);
            Assert.That(data_location, Does.Exist);
            Assert.That(alloc_location, Does.Exist);

            d.Dispose();
            // Directory.Delete(_data_dir, true);
        }

    }
}
