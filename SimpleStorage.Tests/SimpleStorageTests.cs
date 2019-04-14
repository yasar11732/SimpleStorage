using NUnit.Framework;
using System;
using System.IO;
using System.IO.Abstractions.TestingHelpers;
using System.Linq;
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
        public void Collection_RemoveOlderThan()
        {
            Database d = new Database(null);
        }

        public static string RandomString(int length)
        {
            Random random = new Random();
            const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
            return new string(Enumerable.Repeat(chars, length)
              .Select(s => s[random.Next(s.Length)]).ToArray());
        }

        [Test]
        public void Collection_PutGetRemove()
        {
            Database d = new Database(null);
            Random r = new Random();

            var randombytes1 = new byte[r.Next(1, 8192)];
            var randombytes2 = new byte[r.Next(1, 8192)];
            var randombytes3 = new byte[r.Next(1, 8192)];
            var randombytes4 = new byte[r.Next(1, 8192)];
            var randombytes5 = new byte[r.Next(1, 8192)];
            var randombytes6 = new byte[r.Next(1, 8192)];
            var randombytes7 = new byte[r.Next(1, 8192)];
            var randombytes8 = new byte[r.Next(1, 8192)];
            var randombytes9 = new byte[r.Next(1, 8192)];
            var randombytes10 = new byte[r.Next(1, 8192)];

            r.NextBytes(randombytes1);
            r.NextBytes(randombytes2);
            r.NextBytes(randombytes3);
            r.NextBytes(randombytes4);
            r.NextBytes(randombytes5);
            r.NextBytes(randombytes6);
            r.NextBytes(randombytes7);
            r.NextBytes(randombytes8);
            r.NextBytes(randombytes9);
            r.NextBytes(randombytes10);

            var longkey = RandomString(300);

            d.Put("test", longkey, new byte[1]);

            d.Put("test", "key1", randombytes1);
            d.Put("test", "key2", randombytes2);
            d.Put("test", "key3", randombytes3);
            d.Remove("test", "key2");

            d.Put("test", "key4", randombytes4);
            d.Put("test", "key5", randombytes5);
            d.Put("test", "key6", randombytes6);
            d.Remove("test", "key5");

            d.Put("test", "key7", randombytes7);
            d.Put("test", "key8", randombytes8);
            d.Put("test", "key9", randombytes9);
            d.Remove("test", "key8");

            d.Put("test", "key10", randombytes10);

            Assert.That(d.Get("test", "key1"), Is.EqualTo(randombytes1));
            Assert.That(d.Get("test", "key2"), Is.Null);
            Assert.That(d.Get("test", "key3"), Is.EqualTo(randombytes3));
            Assert.That(d.Get("test", "key4"), Is.EqualTo(randombytes4));
            Assert.That(d.Get("test", "key5"), Is.Null);
            Assert.That(d.Get("test", "key6"), Is.EqualTo(randombytes6));
            Assert.That(d.Get("test", "key7"), Is.EqualTo(randombytes7));
            Assert.That(d.Get("test", "key8"), Is.Null);
            Assert.That(d.Get("test", "key9"), Is.EqualTo(randombytes9));
            Assert.That(d.Get("test", "key10"), Is.EqualTo(randombytes10));

            // Directory.Delete(_data_dir, true);
        }

    }
}
