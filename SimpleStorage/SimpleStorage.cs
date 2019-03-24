using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;

namespace SimpleStorage
{


    public class Database
    {
        private string _data_directory = null;

        public string data_directory
        {
            get
            {
                return _data_directory;
            }
        }

        public static string RandomFilename()
        {
            return Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        }

        public Database(string data_directory)
        {
            if(data_directory == null)
            {
                data_directory = RandomFilename();
            }

            _data_directory = data_directory;
            Directory.CreateDirectory(_data_directory);
        }

        public void Put(string collection, UInt64 key, byte[] data)
        {

        }

        public byte[] Get(string collection, UInt64 key)
        {
            return null;
        }

        public void Remove(string collection, UInt64 key)
        {

        }
    }
}
