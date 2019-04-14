using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Abstractions;
using System.IO.MemoryMappedFiles;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;

namespace SimpleStorage
{


    /// <summary>
    /// Database is a collection of collections
    /// </summary>
    public class Database : IDisposable
    {
        private string _data_directory = null;
        private Dictionary<string, Collection> _collections;
        private FileStream _lockfile;

        public string data_directory
        {
            get
            {
                return _data_directory;
            }
        }
        /// <summary>
        /// Generate Random Filename in %temp% directory
        /// </summary>
        /// <returns>Filename in Temp Direcotur</returns>
        public static string RandomFilename()
        {
            return Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        }


        public static string RandomDirName()
        {
            return Path.Combine(Path.GetTempPath(), Path.GetRandomFileName().Split('.')[0]);
        }

        public Database(string data_directory)
        {
            _collections = new Dictionary<string, Collection>();
            if(data_directory == null)
            {
                data_directory = RandomDirName();
            }

            _data_directory = data_directory;
            Directory.CreateDirectory(_data_directory);
            _lockfile = File.Open(Path.Combine(_data_directory, "lock"), FileMode.OpenOrCreate, FileAccess.Read, FileShare.None);
        }

        /// <summary>
        /// Store value in collection
        /// </summary>
        /// <param name="collection">Name of the collection to use, will be automatically created the first time it is accessed</param>
        /// <param name="key">64 bit hash value that will be used to retrieve the data</param>
        /// <param name="data">Data to save</param>
        public void Put(string collection, string key, byte[] data)
        {
            Collection _c = null;

            lock(_collections)
            {
                if (!_collections.TryGetValue(collection, out _c))
                {
                    _c = new Collection(_data_directory, collection);
                    _collections[collection] = _c;
                }
            }


            lock(_c)
            {
                _c.Put(key, data);
            }
        }

        /// <summary>
        /// Restore value from a collection
        /// </summary>
        /// <param name="collection">Collection to search</param>
        /// <param name="key">Key to search for</param>
        /// <returns>Byte array containing the returned Data</returns>
        public byte[] Get(string collection, string key)
        {
            Collection _c = null;

            lock (_collections)
            {
                if (!_collections.TryGetValue(collection, out _c))
                {
                    _c = new Collection(_data_directory, collection);
                    _collections[collection] = _c;
                }
            }

            lock(_c)
            {
                return _c.Get(key);
            }
        }

        /// <summary>
        /// Removes a key from collection
        /// </summary>
        /// <param name="collection">Collection to search</param>
        /// <param name="key">Key to remove</param>
        public void Remove(string collection, string key)
        {
            Collection _c = null;

            lock (_collections)
            {
                if (!_collections.TryGetValue(collection, out _c))
                {
                    _c = new Collection(_data_directory, collection);
                    _collections[collection] = _c;
                }
            }


            lock(_c)
            {
                _c.Remove(key);
            }
        }

        public byte[] GetOrExpire(string collection, string key, TimeSpan s)
        {
            Collection _c = null;

            lock (_collections)
            {
                if (!_collections.TryGetValue(collection, out _c))
                {
                    _c = new Collection(_data_directory, collection);
                    _collections[collection] = _c;
                }
            }


            lock (_c)
            {
                return _c.GetOrExpire(key, s);
            }
        }

        /// <summary>
        /// Removes all keys older than specified time span
        /// </summary>
        public void RemoveOlderThan(string collection, TimeSpan s)
        {
            Collection _c = null;

            lock (_collections)
            {
                if (!_collections.TryGetValue(collection, out _c))
                {
                    throw new ArgumentException("Collection not found");
                }
            }


            lock (_c)
            {
                _c.RemoveOlderThan(s);
            }
        }
        public void Dispose()
        {
            foreach(var _c in _collections.Keys)
            {
                _collections[_c].Dispose();
            }

            _collections = null;
            _lockfile.Dispose();
            _lockfile = null;
        }
    }
}
