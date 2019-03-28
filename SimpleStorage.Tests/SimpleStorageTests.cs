using NUnit.Framework;
using System;
using System.IO;
using System.IO.Abstractions.TestingHelpers;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;

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
        [NonParallelizable]
        public void Collection_CreatesDefaultFiles()
        {
            Database d = new Database(null);
            var randombytes = new byte[1098];
            Random r = new Random();

            r.NextBytes(randombytes);

            d.Put("test_collection","Yaşar1", Encoding.UTF8.GetBytes("Bunu yazan tosun, okuyana, muhahahah!!!1"));
            d.Put("test_collection", "Yaşar2", Encoding.UTF8.GetBytes("Bunu yazan tosun, okuyana, muhahahah!!!2"));
            d.Put("test_collection", "Yaşar3", Encoding.UTF8.GetBytes("Bunu yazan tosun, okuyana, muhahahah!!!3"));
            d.Put("test_collection", "Yaşar7", randombytes);
            d.Put("test_collection", "Yaşar4", Encoding.UTF8.GetBytes("Bunu yazan tosun, okuyana, muhahahah!!!4"));
            d.Put("test_collection", "Yaşar5", Encoding.UTF8.GetBytes("Bunu yazan tosun, okuyana, muhahahah!!!5"));
            d.Put("test_collection", "Yaşar6", Encoding.UTF8.GetBytes("Bunu yazan tosun, okuyana, muhahahah!!!6"));

            var testbytes = d.Get("test_collection","Yaşar7");
            Assert.That(testbytes, Is.EqualTo(randombytes));

            // d.Put("test_collection", "Yaşar7", Encoding.UTF8.GetBytes("Bunu yazan tosun, okuyana, muhahahah!!!7"));
            // d.Put("test_collection", "Yaşar8", Encoding.UTF8.GetBytes("Bunu yazan tosun, okuyana, muhahahah!!!8"));
            // d.Put("test_collection", "Yaşar9", Encoding.UTF8.GetBytes("Bunu yazan tosun, okuyana, muhahahah!!!9"));

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
